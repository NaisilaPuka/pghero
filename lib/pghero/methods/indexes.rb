module PgHero
  module Methods
    module Indexes
      include Citus

      def index_hit_rate
        if !citus_enabled?
          select_one <<-SQL
            SELECT
              (sum(idx_blks_hit)) / nullif(sum(idx_blks_hit + idx_blks_read), 0) AS rate
            FROM
              pg_statio_user_indexes
          SQL
        else
          select_one <<-SQL
            WITH worker_idx_blks_stats_json AS (
              SELECT
                result::json
              FROM
                run_command_on_workers($$
                SELECT
                  json_agg(row_to_json(worker_idx_blks_stats))
                FROM
                  (
                    SELECT
                      sum(idx_blks_hit) AS idx_blks_hit,
                      sum(idx_blks_read) AS idx_blks_read
                    FROM
                      pg_statio_user_indexes
                  )
                  worker_idx_blks_stats $$)
            )
            SELECT
              (sum(idx_blks_hit)) / nullif(sum(idx_blks_hit + idx_blks_read), 0) AS rate
            FROM
              (
                SELECT
                  idx_blks_hit,
                  idx_blks_read
                FROM
                  pg_statio_user_indexes
                UNION ALL
                SELECT
                  (json_array_elements(result)->>'idx_blks_hit')::bigint AS idx_blks_hit,
                  (json_array_elements(result)->>'idx_blks_read')::bigint AS idx_blks_read
                FROM
                  worker_idx_blks_stats_json
              ) AS cluster_idx_blks_stats
          SQL
        end
      end

      def index_caching
        if !citus_enabled?
          select_all <<-SQL
            SELECT
              schemaname AS schema,
              relname AS table,
              indexrelname AS index,
              CASE WHEN idx_blks_hit + idx_blks_read = 0 THEN
                0
              ELSE
                ROUND(1.0 * idx_blks_hit / (idx_blks_hit + idx_blks_read), 2)
              END AS hit_rate
            FROM
              pg_statio_user_indexes
            ORDER BY
              3 DESC, 1
          SQL
        else
          select_all <<-SQL
            WITH dist_indexes AS (
              SELECT
                indexname,
                logicalrelid,
                schemaname,
                tablename
              FROM
                pg_indexes
                JOIN pg_dist_partition ON (schemaname || '.' || tablename)::regclass = logicalrelid
              ORDER BY 
                1, 4, 3
            ),
            placements_idx_blks_hit_read AS (
              SELECT
                indexname,
                (run_command_on_placements(logicalrelid, $$
                  SELECT
                    idx_blks_hit
                  FROM
                    pg_statio_user_indexes
                  WHERE
                    indexrelname = replace('%s','$$ || logicalrelid::text || $$', '$$ || indexname::text || $$') $$ )).result::bigint AS idx_blks_hit,
                (run_command_on_placements(logicalrelid, $$
                  SELECT
                    idx_blks_read
                  FROM
                    pg_statio_user_indexes
                  WHERE
                    indexrelname = replace('%s','$$ || logicalrelid::text || $$', '$$ || indexname::text || $$') $$ )).result::bigint AS idx_blks_read
              FROM
                dist_indexes
            )
            SELECT
              schemaname AS schema,
              relname AS table,
              indexrelname AS index,
              CASE WHEN idx_blks_hit + idx_blks_read = 0 THEN
                0
              ELSE
                ROUND(1.0 * idx_blks_hit / (idx_blks_hit + idx_blks_read), 2)
              END AS hit_rate
            FROM (
              SELECT
                schemaname,
                tablename AS relname,
                p.indexname as indexrelname,
                sum(idx_blks_hit) AS idx_blks_hit,
                sum(idx_blks_read) AS idx_blks_read
              FROM
                placements_idx_blks_hit_read p
                JOIN dist_indexes d ON p.indexname = d.indexname
              GROUP BY
                3, 2, 1
              UNION ALL
              SELECT
                schemaname,
                relname,
                indexrelname,
                idx_blks_hit,
                idx_blks_read
              FROM
                pg_statio_user_indexes s
              WHERE
                NOT EXISTS(SELECT * FROM dist_indexes WHERE indexname = indexrelname AND tablename = relname AND schemaname = s.schemaname)
            ) all_stats
            ORDER BY
              3 DESC, 1
          SQL
        end
      end

      def index_usage
        if !citus_enabled?
          select_all <<-SQL
            SELECT
              schemaname AS schema,
              relname AS table,
              CASE idx_scan
                WHEN 0 THEN 'Insufficient data'
                ELSE (100 * idx_scan / (seq_scan + idx_scan))::text
              END percent_of_times_index_used,
              n_live_tup AS estimated_rows
            FROM
              pg_stat_user_tables
            ORDER BY
              n_live_tup DESC,
              relname ASC
           SQL
        else
          select_all <<-SQL
            WITH dist_tables_with_indexes (tablename) AS (
              SELECT 
                DISTINCT indrelid::regclass
              FROM
                pg_index
                JOIN pg_dist_partition ON indrelid = logicalrelid
            ),
            placement_stats_json AS (
              SELECT
                tablename,
                (run_command_on_placements(tablename, $$ SELECT json_agg(row_to_json(placement_stats)) FROM (
                  SELECT
                    idx_scan, seq_scan, n_live_tup
                  FROM
                    pg_stat_user_tables
                  WHERE
                    relid = '%s'::regclass::oid) placement_stats $$ )).result::json AS stats
              FROM
                dist_tables_with_indexes
            ),
            placement_stats AS (
            SELECT
              tablename,
              (json_array_elements(stats)->>'idx_scan')::bigint AS idx_scan,
              (json_array_elements(stats)->>'seq_scan')::bigint AS seq_scan,
              (json_array_elements(stats)->>'n_live_tup')::bigint AS n_live_tup
            FROM
              placement_stats_json
            ORDER BY
              tablename
            )
            SELECT
              schemaname AS schema,
              relname AS table,
              CASE idx_scan
                WHEN 0 THEN 'Insufficient data'
                ELSE (100 * idx_scan / (seq_scan + idx_scan))::text
              END percent_of_times_index_used,
              n_live_tup AS estimated_rows
            FROM (
              SELECT
                n.nspname AS schemaname,
                c.relname,
                sum(idx_scan) AS idx_scan,
                sum(seq_scan) AS seq_scan,
                sum(n_live_tup) AS n_live_tup
              FROM
                placement_stats p
                JOIN pg_class c ON p.tablename::oid = c.oid
                LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
              GROUP BY
                2, 1
              UNION ALL
              SELECT
                schemaname,
                relname,
                idx_scan,
                seq_scan,
                n_live_tup
              FROM
                pg_stat_user_tables s
              WHERE
                NOT EXISTS(SELECT * FROM dist_tables_with_indexes WHERE tablename = (schemaname || '.' || relname)::regclass)
            ) all_stats
            ORDER BY
              n_live_tup DESC,
              relname ASC
          SQL
        end
      end

      def missing_indexes
        if !citus_enabled?
          select_all <<-SQL
            SELECT
              schemaname AS schema,
              relname AS table,
              CASE idx_scan
                WHEN 0 THEN 'Insufficient data'
                ELSE (100 * idx_scan / (seq_scan + idx_scan))::text
              END percent_of_times_index_used,
              n_live_tup AS estimated_rows
            FROM
              pg_stat_user_tables
            WHERE
              idx_scan > 0
              AND (100 * idx_scan / (seq_scan + idx_scan)) < 95
              AND n_live_tup >= 10000
            ORDER BY
              n_live_tup DESC,
              relname ASC
           SQL
        else
          select_all <<-SQL
            WITH dist_tables_with_indexes (tablename) AS (
              SELECT 
                DISTINCT indrelid::regclass
              FROM
                pg_index
                JOIN pg_dist_partition ON indrelid = logicalrelid
            ),
            placement_stats_json AS (
              SELECT
                tablename,
                (run_command_on_placements(tablename, $$ SELECT json_agg(row_to_json(placement_stats)) FROM (
                  SELECT
                    idx_scan, seq_scan, n_live_tup
                  FROM
                    pg_stat_user_tables
                  WHERE
                    relid = '%s'::regclass::oid) placement_stats $$ )).result::json AS stats
              FROM
                dist_tables_with_indexes
            ),
            placement_stats AS (
              SELECT
                tablename,
                (json_array_elements(stats)->>'idx_scan')::bigint AS idx_scan,
                (json_array_elements(stats)->>'seq_scan')::bigint AS seq_scan,
                (json_array_elements(stats)->>'n_live_tup')::bigint AS n_live_tup
              FROM
                placement_stats_json
              ORDER BY
                tablename
            )
            SELECT
              schemaname AS schema,
              relname AS table,
              CASE idx_scan
                WHEN 0 THEN 'Insufficient data'
                ELSE (100 * idx_scan / (seq_scan + idx_scan))::text
              END percent_of_times_index_used,
              n_live_tup AS estimated_rows
            FROM (
              SELECT
                n.nspname AS schemaname,
                c.relname,
                sum(idx_scan) AS idx_scan,
                sum(seq_scan) AS seq_scan,
                sum(n_live_tup) AS n_live_tup
              FROM
                placement_stats p
                JOIN pg_class c ON p.tablename::oid = c.oid
                LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
              GROUP BY
                2, 1
              UNION ALL
              SELECT
                schemaname,
                relname,
                idx_scan,
                seq_scan,
                n_live_tup
              FROM
                pg_stat_user_tables s
              WHERE
                NOT EXISTS(SELECT * FROM dist_tables_with_indexes WHERE tablename = (schemaname || '.' || relname)::regclass)
            ) all_stats
            WHERE
              idx_scan > 0
              AND (100 * idx_scan / (seq_scan + idx_scan)) < 95
              AND n_live_tup >= 10000
            ORDER BY
              n_live_tup DESC,
              relname ASC
          SQL
        end
      end

      def unused_indexes(max_scans: 50, across: [])
        if !citus_enabled?
          result = select_all_size <<-SQL
            SELECT
              schemaname AS schema,
              relname AS table,
              indexrelname AS index,
              pg_relation_size(i.indexrelid) AS size_bytes,
              idx_scan as index_scans
            FROM
              pg_stat_user_indexes ui
            INNER JOIN
              pg_index i ON ui.indexrelid = i.indexrelid
            WHERE
              NOT indisunique
              AND idx_scan <= #{max_scans.to_i}
            ORDER BY
              pg_relation_size(i.indexrelid) DESC,
              relname ASC
          SQL
        else
          result = select_all_size <<-SQL
            WITH dist_indexes AS (
              SELECT
                indexname,
                logicalrelid,
                schemaname,
                tablename
              FROM
                pg_indexes
                JOIN pg_dist_partition ON tablename = logicalrelid::name OR schemaname::text || '.' || tablename::text = logicalrelid::text
            ),
            placement_idx_sizes_scans AS (
              SELECT
                indexname,
                1 AS count,
                (run_command_on_placements(logicalrelid, $$
                  SELECT
                    pg_relation_size('$$ || schemaname::text || $$' || '.' || replace('%s','$$ || logicalrelid::text || $$', '$$ || indexname::text || $$')) $$ )).result::bigint AS placement_idx_size,
                (run_command_on_placements(logicalrelid, $$
                  SELECT
                    idx_scan
                  FROM
                    pg_stat_user_indexes
                  WHERE
                    indexrelname = replace('%s','$$ || logicalrelid::text || $$', '$$ || indexname::text || $$') $$ )).result::bigint AS idx_scan
              FROM
                dist_indexes
            ),
            dist_index_size_scan AS (
              SELECT
                schemaname,
                tablename AS relname,
                p.indexname AS indexrelname,
                sum(placement_idx_size) AS size_bytes,
                sum(idx_scan) / sum(count) AS idx_scan,
                (SELECT indexrelid FROM pg_stat_user_indexes ui WHERE ui.schemaname = schemaname AND ui.relname = tablename AND ui.indexrelname = p.indexname) AS indexrelid
              FROM
                placement_idx_sizes_scans p
                JOIN dist_indexes d ON p.indexname = d.indexname
              GROUP BY
                3, 2, 1
              ORDER BY
                3
            )
            SELECT
              schemaname AS schema,
              relname AS table,
              indexrelname AS index,
              size_bytes,
              idx_scan as index_scans
            FROM (
              SELECT
                schemaname,
                tablename AS relname,
                p.indexname AS indexrelname,
                sum(placement_idx_size) AS size_bytes,
                sum(idx_scan) / sum(count) AS idx_scan,
                (SELECT indexrelid FROM pg_stat_user_indexes ui WHERE ui.schemaname = schemaname AND ui.relname = tablename AND ui.indexrelname = p.indexname) AS indexrelid
              FROM
                placement_idx_sizes_scans p
                JOIN dist_indexes d ON p.indexname = d.indexname
              GROUP BY
                3, 2, 1
              UNION ALL
              SELECT
                schemaname,
                relname,
                indexrelname,
                pg_relation_size(indexrelid) AS size_bytes,
                idx_scan,
                indexrelid
              FROM
                pg_stat_user_indexes s
              WHERE
                NOT EXISTS(SELECT * FROM dist_indexes WHERE indexname = s.indexrelname AND tablename = relname AND schemaname = s.schemaname)
            ) ui
            INNER JOIN
              pg_index i ON ui.indexrelid = i.indexrelid
            WHERE
              NOT indisunique
              AND idx_scan <= #{max_scans.to_i}
            ORDER BY
              size_bytes DESC,
              relname ASC
          SQL
        end

        across.each do |database_id|
          database = PgHero.databases.values.find { |d| d.id == database_id }
          raise PgHero::Error, "Database not found: #{database_id}" unless database
          across_result = Set.new(database.unused_indexes(max_scans: max_scans).map { |v| [v[:schema], v[:index]] })
          result.select! { |v| across_result.include?([v[:schema], v[:index]]) }
        end

        result
      end

      def reset_stats
        if !citus_enabled?
          execute("SELECT pg_stat_reset()")
          true
        else
          execute("SELECT pg_stat_reset() UNION ALL SELECT (run_command_on_workers($$ SELECT pg_stat_reset() $$)).result::void AS pg_stat_reset")
          true          
        end
      end

      def last_stats_reset_time
        select_one <<-SQL
          SELECT
            pg_stat_get_db_stat_reset_time(oid) AS reset_time
          FROM
            pg_database
          WHERE
            datname = current_database()
        SQL
      end

      def invalid_indexes
        select_all <<-SQL
          SELECT
            n.nspname AS schema,
            c.relname AS index,
            pg_get_indexdef(i.indexrelid) AS definition
          FROM
            pg_catalog.pg_class c,
            pg_catalog.pg_namespace n,
            pg_catalog.pg_index i
          WHERE
            i.indisvalid = false
            AND i.indexrelid = c.oid
            AND c.relnamespace = n.oid
            AND n.nspname != 'pg_catalog'
            AND n.nspname != 'information_schema'
            AND n.nspname != 'pg_toast'
          ORDER BY
            c.relname
        SQL
      end

      # TODO parse array properly
      # https://stackoverflow.com/questions/2204058/list-columns-with-indexes-in-postgresql
      def indexes
        select_all(<<-SQL
          SELECT
            schemaname AS schema,
            t.relname AS table,
            ix.relname AS name,
            regexp_replace(pg_get_indexdef(i.indexrelid), '^[^\\(]*\\((.*)\\)$', '\\1') AS columns,
            regexp_replace(pg_get_indexdef(i.indexrelid), '.* USING ([^ ]*) \\(.*', '\\1') AS using,
            indisunique AS unique,
            indisprimary AS primary,
            indisvalid AS valid,
            indexprs::text,
            indpred::text,
            pg_get_indexdef(i.indexrelid) AS definition
          FROM
            pg_index i
          INNER JOIN
            pg_class t ON t.oid = i.indrelid
          INNER JOIN
            pg_class ix ON ix.oid = i.indexrelid
          LEFT JOIN
            pg_stat_user_indexes ui ON ui.indexrelid = i.indexrelid
          WHERE
            schemaname IS NOT NULL
          ORDER BY
            1, 2
        SQL
        ).map { |v| v[:columns] = v[:columns].sub(") WHERE (", " WHERE ").split(", ").map { |c| unquote(c) }; v }
      end

      def duplicate_indexes(indexes: nil)
        dup_indexes = []

        indexes_by_table = (indexes || self.indexes).group_by { |i| i[:table] }
        indexes_by_table.values.flatten.select { |i| !i[:primary] && !i[:unique] && !i[:indexprs] && !i[:indpred] && i[:valid] }.each do |index|
          covering_index = indexes_by_table[index[:table]].find { |i| index_covers?(i[:columns], index[:columns]) && i[:using] == index[:using] && i[:name] != index[:name] && i[:schema] == index[:schema] && !i[:indexprs] && !i[:indpred] && i[:valid] }
          if covering_index && (covering_index[:columns] != index[:columns] || index[:name] > covering_index[:name])
            dup_indexes << {unneeded_index: index, covering_index: covering_index}
          end
        end

        dup_indexes.sort_by { |i| ui = i[:unneeded_index]; [ui[:table], ui[:columns]] }
      end

      # https://gist.github.com/mbanck/9976015/71888a24e464e2f772182a7eb54f15a125edf398
      # thanks @jberkus and @mbanck
      def index_bloat(min_size: nil)
        min_size ||= index_bloat_bytes
        if !citus_enabled?
          select_all <<-SQL
            WITH btree_index_atts AS (
              SELECT
                nspname, relname, reltuples, relpages, indrelid, relam,
                regexp_split_to_table(indkey::text, ' ')::smallint AS attnum,
                indexrelid as index_oid
              FROM
                pg_index
              JOIN
                pg_class ON pg_class.oid=pg_index.indexrelid
              JOIN
                pg_namespace ON pg_namespace.oid = pg_class.relnamespace
              JOIN
                pg_am ON pg_class.relam = pg_am.oid
              WHERE
                pg_am.amname = 'btree'
            ),
            index_item_sizes AS (
              SELECT
                i.nspname,
                i.relname,
                i.reltuples,
                i.relpages,
                i.relam,
                (quote_ident(s.schemaname) || '.' || quote_ident(s.tablename))::regclass AS starelid,
                a.attrelid AS table_oid, index_oid,
                current_setting('block_size')::numeric AS bs,
                /* MAXALIGN: 4 on 32bits, 8 on 64bits (and mingw32 ?) */
                CASE
                  WHEN version() ~ 'mingw32' OR version() ~ '64-bit' THEN 8
                  ELSE 4
                END AS maxalign,
                24 AS pagehdr,
                /* per tuple header: add index_attribute_bm if some cols are null-able */
                CASE WHEN max(coalesce(s.null_frac,0)) = 0
                  THEN 2
                  ELSE 6
                END AS index_tuple_hdr,
                /* data len: we remove null values save space using it fractionnal part from stats */
                sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 2048) ) AS nulldatawidth
              FROM
                pg_attribute AS a
              JOIN
                pg_stats AS s ON (quote_ident(s.schemaname) || '.' || quote_ident(s.tablename))::regclass=a.attrelid AND s.attname = a.attname
              JOIN
                btree_index_atts AS i ON i.indrelid = a.attrelid AND a.attnum = i.attnum
              WHERE
                a.attnum > 0
              GROUP BY
                1, 2, 3, 4, 5, 6, 7, 8, 9
            ),
            index_aligned AS (
              SELECT
                maxalign,
                bs,
                nspname,
                relname AS index_name,
                reltuples,
                relpages,
                relam,
                table_oid,
                index_oid,
                ( 2 +
                  maxalign - CASE /* Add padding to the index tuple header to align on MAXALIGN */
                    WHEN index_tuple_hdr%maxalign = 0 THEN maxalign
                    ELSE index_tuple_hdr%maxalign
                  END
                + nulldatawidth + maxalign - CASE /* Add padding to the data to align on MAXALIGN */
                    WHEN nulldatawidth::integer%maxalign = 0 THEN maxalign
                    ELSE nulldatawidth::integer%maxalign
                  END
                )::numeric AS nulldatahdrwidth, pagehdr
              FROM
                index_item_sizes AS s1
            ),
            otta_calc AS (
              SELECT
                bs,
                nspname,
                table_oid,
                index_oid,
                index_name,
                relpages,
                coalesce(
                  ceil((reltuples*(4+nulldatahdrwidth))/(bs-pagehdr::float)) +
                  CASE WHEN am.amname IN ('hash','btree') THEN 1 ELSE 0 END , 0 /* btree and hash have a metadata reserved block */
                ) AS otta
              FROM
                index_aligned AS s2
              LEFT JOIN
                pg_am am ON s2.relam = am.oid
            ),
            raw_bloat AS (
              SELECT
                nspname,
                c.relname AS table_name,
                index_name,
                bs*(sub.relpages)::bigint AS totalbytes,
                CASE
                  WHEN sub.relpages <= otta THEN 0
                  ELSE bs*(sub.relpages-otta)::bigint END
                  AS wastedbytes,
                CASE
                  WHEN sub.relpages <= otta
                  THEN 0 ELSE bs*(sub.relpages-otta)::bigint * 100 / (bs*(sub.relpages)::bigint) END
                  AS realbloat,
                pg_relation_size(sub.table_oid) as table_bytes,
                stat.idx_scan as index_scans,
                stat.indexrelid
              FROM
                otta_calc AS sub
              JOIN
                pg_class AS c ON c.oid=sub.table_oid
              JOIN
                pg_stat_user_indexes AS stat ON sub.index_oid = stat.indexrelid
            )
            SELECT
              nspname AS schema,
              table_name AS table,
              index_name AS index,
              wastedbytes AS bloat_bytes,
              totalbytes AS index_bytes,
              pg_get_indexdef(rb.indexrelid) AS definition,
              indisprimary AS primary
            FROM
              raw_bloat rb
            INNER JOIN
              pg_index i ON i.indexrelid = rb.indexrelid
            WHERE
              wastedbytes >= #{min_size.to_i}
            ORDER BY
              wastedbytes DESC,
              index_name
          SQL
        else
          select_all <<-SQL
            WITH dist_indexes AS (
              SELECT
                indexname,
                logicalrelid,
                schemaname,
                tablename
              FROM
                pg_indexes
              JOIN
                pg_dist_partition ON (schemaname || '.' || tablename)::regclass = logicalrelid
            ),
            dist_index_reltuples_relpages AS (
              SELECT
                indexname,
                (run_command_on_placements(logicalrelid, $$
                  SELECT
                    reltuples
                  FROM
                    pg_class
                  WHERE
                    relname = replace('%s','$$ || logicalrelid::text || $$', '$$ || indexname::text || $$') $$ )).result::bigint AS reltuples,
                (run_command_on_placements(logicalrelid, $$
                  SELECT
                    relpages
                  FROM
                    pg_class
                  WHERE
                    relname = replace('%s','$$ || logicalrelid::text || $$', '$$ || indexname::text || $$') $$ )).result::bigint AS relpages
              FROM
                dist_indexes
            ),
            btree_index_atts AS (
              SELECT
                nspname, relname, reltuples, relpages, indrelid, relam,
                regexp_split_to_table(indkey::text, ' ')::smallint AS attnum,
                indexrelid as index_oid
              FROM
                pg_index
              JOIN (
                SELECT
                  c.oid,
                  relnamespace,
                  relam,
                  di.indexname AS relname,
                  sum(di.reltuples) AS reltuples,
                  sum(di.relpages) AS relpages
                FROM
                  dist_index_reltuples_relpages di
                JOIN
                  dist_indexes d ON di.indexname = d.indexname
                JOIN
                  pg_class c ON (schemaname || '.' || di.indexname)::regclass::oid = c.oid
                GROUP BY
                  4, 1, 3, 2
                UNION ALL
                SELECT
                  oid,
                  relnamespace,
                  relam,
                  relname,
                  reltuples,
                  relpages
                FROM
                  pg_class c
                WHERE
                  NOT EXISTS(SELECT * FROM dist_indexes WHERE (schemaname || '.' || indexname)::regclass::oid = c.oid)
              ) pg_class_with_dist ON pg_class_with_dist.oid=pg_index.indexrelid
              JOIN
                pg_namespace ON pg_namespace.oid = pg_class_with_dist.relnamespace
              JOIN
                pg_am ON pg_class_with_dist.relam = pg_am.oid
              WHERE
                pg_am.amname = 'btree'
            ),
            pg_dist_stats_double_json AS (
              SELECT (
                SELECT
                  json_agg(row_to_json(f))
                FROM (
                  SELECT
                    result
                  FROM
                    run_command_on_placements(logicalrelid, $$
                      SELECT
                        json_agg(row_to_json(d))
                      FROM (
                        SELECT
                          '$$ || logicalrelid || $$' AS dist_table,
                          schemaname,
                          attname,
                          null_frac,
                          reltuples,
                          avg_width
                        FROM
                          pg_stats s
                        JOIN
                          pg_class c ON s.tablename = c.relname
                        WHERE
                          c.oid = '%s'::regclass::oid
                      ) d $$)
                ) f)
              FROM
                pg_dist_partition
            ),
            pg_dist_stats_single_json AS (
              SELECT
                (json_array_elements(json_agg)->>'result') AS result
              FROM
                pg_dist_stats_double_json
            ),
            pg_dist_stats_regular AS (
              SELECT
                (json_array_elements(result::json)->>'dist_table')::regclass AS dist_table,
                (json_array_elements(result::json)->>'schemaname')::name AS schemaname,
                (json_array_elements(result::json)->>'attname')::name AS attname,
                (json_array_elements(result::json)->>'null_frac')::real AS null_frac,
                (json_array_elements(result::json)->>'reltuples')::integer AS reltuples,
                (json_array_elements(result::json)->>'avg_width')::integer AS avg_width
              FROM
                pg_dist_stats_single_json
              WHERE
                result != ''
            ),
            index_item_sizes AS (
              SELECT
                i.nspname,
                i.relname,
                i.reltuples,
                i.relpages,
                i.relam,
                (quote_ident(s.schemaname) || '.' || quote_ident(s.tablename))::regclass AS starelid,
                a.attrelid AS table_oid, index_oid,
                current_setting('block_size')::numeric AS bs,
                /* MAXALIGN: 4 on 32bits, 8 on 64bits (and mingw32 ?) */
                CASE
                  WHEN version() ~ 'mingw32' OR version() ~ '64-bit' THEN 8
                  ELSE 4
                END AS maxalign,
                24 AS pagehdr,
                /* per tuple header: add index_attribute_bm if some cols are null-able */
                CASE WHEN max(coalesce(s.null_frac,0)) = 0
                  THEN 2
                  ELSE 6
                END AS index_tuple_hdr,
                /* data len: we remove null values save space using it fractionnal part from stats */
                sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 2048) ) AS nulldatawidth
              FROM
                pg_attribute AS a
              JOIN (
                  SELECT
                    schemaname,
                    c.relname AS tablename,
                    attname,
                    sum(null_frac * t.reltuples) / sum(t.reltuples) AS null_frac,
                    sum(avg_width * t.reltuples) / sum(t.reltuples) AS avg_width
                  FROM
                    pg_dist_stats_regular t
                  JOIN
                    pg_class c ON dist_table::oid = c.oid
                  GROUP BY
                    dist_table,
                    attname,
                    tablename,
                    schemaname
                  UNION ALL
                  SELECT
                    schemaname,
                    tablename,
                    attname,
                    null_frac,
                    avg_width
                  FROM
                    pg_stats
                  WHERE
                    NOT EXISTS(SELECT * FROM pg_dist_partition WHERE logicalrelid = (schemaname || '.' || tablename)::regclass)
                  ) AS s ON (quote_ident(s.schemaname) || '.' || quote_ident(s.tablename))::regclass = a.attrelid AND s.attname = a.attname
              JOIN
                btree_index_atts AS i ON i.indrelid = a.attrelid AND a.attnum = i.attnum
              WHERE
                a.attnum > 0
              GROUP BY
                1, 2, 3, 4, 5, 6, 7, 8, 9
            ),
            index_aligned AS (
              SELECT
                maxalign,
                bs,
                nspname,
                relname AS index_name,
                reltuples,
                relpages,
                relam,
                table_oid,
                index_oid,
                ( 2 +
                  maxalign - CASE /* Add padding to the index tuple header to align on MAXALIGN */
                    WHEN index_tuple_hdr%maxalign = 0 THEN maxalign
                    ELSE index_tuple_hdr%maxalign
                  END
                + nulldatawidth + maxalign - CASE /* Add padding to the data to align on MAXALIGN */
                    WHEN nulldatawidth::integer%maxalign = 0 THEN maxalign
                    ELSE nulldatawidth::integer%maxalign
                  END
                )::numeric AS nulldatahdrwidth, pagehdr
              FROM
                index_item_sizes AS s1
            ),
            otta_calc AS (
              SELECT
                bs,
                nspname,
                table_oid,
                index_oid,
                index_name,
                relpages,
                coalesce(
                  ceil((reltuples*(4+nulldatahdrwidth))/(bs-pagehdr::float)) +
                  CASE WHEN am.amname IN ('hash','btree') THEN 1 ELSE 0 END , 0 /* btree and hash have a metadata reserved block */
                ) AS otta
              FROM
                index_aligned AS s2
              LEFT JOIN
                pg_am am ON s2.relam = am.oid
            ),
            pg_dist_stat_user_indexes AS
            (
              SELECT
                indexname,
                schemaname,
                logicalrelid,
                1 AS placement_count,
                (run_command_on_placements(logicalrelid, $$
                  SELECT
                    idx_scan
                  FROM
                    pg_stat_user_indexes
                  WHERE
                    indexrelname = replace('%s','$$ || logicalrelid::text || $$', '$$ || indexname::text || $$') $$ )).result::bigint AS idx_scan
              FROM
                dist_indexes
            ),
            raw_bloat AS (
              SELECT
                nspname,
                c.relname AS table_name,
                index_name,
                bs*(sub.relpages)::bigint AS totalbytes,
                CASE
                  WHEN sub.relpages <= otta THEN 0
                  ELSE bs*(sub.relpages-otta)::bigint END
                  AS wastedbytes,
                CASE
                  WHEN sub.relpages <= otta
                  THEN 0 ELSE bs*(sub.relpages-otta)::bigint * 100 / (bs*(sub.relpages)::bigint) END
                  AS realbloat,
                stat.table_bytes,
                stat.idx_scan as index_scans,
                stat.indexrelid
              FROM
                otta_calc AS sub
              JOIN
                pg_class AS c ON c.oid=sub.table_oid
              JOIN (
                SELECT
                  (schemaname || '.' || indexname)::regclass::oid AS indexrelid,
                  ceiling(sum(idx_scan)::decimal / sum(placement_count)::decimal)::bigint AS idx_scan,
                  citus_relation_size(logicalrelid) AS table_bytes
                FROM
                  pg_dist_stat_user_indexes
                GROUP BY
                  1, 3
                UNION ALL
                SELECT
                  indexrelid,
                  idx_scan,
                  pg_relation_size((schemaname || '.' || relname)::regclass) AS table_bytes
                FROM
                  pg_stat_user_indexes
                WHERE
                  NOT EXISTS(SELECT * FROM pg_dist_partition WHERE logicalrelid = (schemaname || '.' || relname)::regclass)
              ) AS stat ON sub.index_oid = stat.indexrelid
            )
            SELECT
              nspname AS schema,
              table_name AS table,
              index_name AS index,
              wastedbytes AS bloat_bytes,
              totalbytes AS index_bytes,
              pg_get_indexdef(rb.indexrelid) AS definition,
              indisprimary AS primary
            FROM
              raw_bloat rb
            INNER JOIN
              pg_index i ON i.indexrelid = rb.indexrelid
            WHERE
              wastedbytes >= #{min_size.to_i}
            ORDER BY
              wastedbytes DESC,
              index_name
          SQL
        end
      end
    end
  end
end
