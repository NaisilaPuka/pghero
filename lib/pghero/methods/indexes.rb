module PgHero
  module Methods
    module Indexes
      include Citus

      def index_hit_rate
        if citus_enabled?
          select_one <<-SQL
            WITH worker_index_stats AS(
              SELECT
                result 
              FROM
                run_command_on_workers($cmd$ 
                SELECT
                  json_agg(row_to_json(d)) 
                FROM
                  (
                    SELECT
                      sum(idx_blks_hit) AS hit,
                      nullif(sum(idx_blks_hit + idx_blks_read), 0) AS hit_and_read 
                    FROM
                      pg_statio_user_indexes 
                  )
                  d $cmd$) 
            )
            SELECT
              SUM(hit) / SUM(hit_and_read) AS rate 
            FROM
              (
                SELECT
                  sum(idx_blks_hit) AS hit,
                  nullif(sum(idx_blks_hit + idx_blks_read), 0) AS hit_and_read 
                FROM
                  pg_statio_user_indexes 
                UNION ALL
                SELECT
                  x.hit,
                  x.hit_and_read 
                FROM
                  worker_index_stats 
                CROSS JOIN
                  json_to_recordset(worker_index_stats.result::json) AS x("hit" int, "hit_and_read" int) 
              )
              AS cluster_index_table
          SQL
        else
          select_one <<-SQL
            SELECT
              (sum(idx_blks_hit)) / nullif(sum(idx_blks_hit + idx_blks_read), 0) AS rate
            FROM
              pg_statio_user_indexes
          SQL
        end
      end

      def index_caching
        if citus_enabled?
          select_all <<-SQL
            WITH pg_dist_index_caching AS(
              WITH indices (idxname, tablename) AS(
                SELECT
                  indexrelid,
                  indrelid 
                FROM
                  pg_index 
                JOIN
                pg_dist_partition ON (indrelid = logicalrelid) 
              ),
              dist_indices AS(
                SELECT
                  idxname::regclass,
                  (run_command_on_placements(tablename::regclass, $$ 
                    SELECT
                      idx_blks_hit AS hit 
                    FROM
                      pg_statio_user_indexes 
                    WHERE
                      indexrelname = '$$ || idxname::regclass::text || $$' || replace('%s', '$$ || tablename::regclass::text || $$', '') $$ )).result AS hit,
                  (run_command_on_placements(tablename::regclass, $$ 
                    SELECT
                      idx_blks_hit + idx_blks_read AS hit_and_read 
                    FROM
                      pg_statio_user_indexes 
                    WHERE
                      indexrelname = '$$ || idxname::regclass::text || $$' || replace('%s', '$$ || tablename::regclass::text || $$', '') $$ )).result AS hit_and_read 
                FROM
                  indices 
              )
              SELECT
                idxname,
                CASE WHEN sum(hit_and_read::decimal) = 0 THEN
                    0 
                  ELSE
                    ROUND(1.0 * sum(hit::decimal) / sum(hit_and_read::decimal), 2) 
                END AS hit_rate 
              FROM
                dist_indices 
              GROUP BY
                idxname 
              ORDER BY
                idxname 
            )
            SELECT
              schemaname AS SCHEMA,
              relname AS TABLE,
              indexrelname AS INDEX,
              CASE WHEN (( 
                    SELECT
                      count(*) 
                    FROM
                      pg_dist_index_caching 
                    WHERE
                      idxname::NAME = indexrelname) > 0)
                THEN ( 
                  SELECT
                    hit_rate 
                  FROM
                    pg_dist_index_caching 
                  WHERE
                    idxname::NAME = indexrelname LIMIT 1) 
                ELSE
                  CASE WHEN idx_blks_hit + idx_blks_read = 0 THEN
                    0 
                  ELSE
                    ROUND(1.0 * idx_blks_hit / (idx_blks_hit + idx_blks_read), 2) 
                  END
              END AS hit_rate 
            FROM
              pg_statio_user_indexes 
            ORDER BY
              3 DESC, 1
          SQL
        else
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
        end
      end

      def index_usage
        if citus_enabled?
          select_all <<-SQL
            WITH pg_dist_index_usage AS(
              WITH dist_tables_with_indexes (tablename) AS(
                SELECT
                  indrelid 
                FROM
                  pg_index 
                JOIN
                  pg_dist_partition ON ( indrelid = logicalrelid) 
              ),
              dist_indexes AS(
                SELECT
                  tablename::regclass,
                  (run_command_on_placements(tablename::regclass, $$ 
                    SELECT
                      idx_scan 
                    FROM
                      pg_stat_user_tables 
                    WHERE
                      relname = '%s' $$ )).result AS idx_scan,
                  (run_command_on_placements(tablename::regclass, $$ 
                    SELECT
                      seq_scan + idx_scan AS idx_and_seq_scan 
                    FROM
                      pg_stat_user_tables 
                    WHERE
                      relname = '%s' $$ )).result AS idx_and_seq_scan,
                  (run_command_on_placements(tablename::regclass, $$ 
                    SELECT
                      n_live_tup 
                    FROM
                      pg_stat_user_tables 
                    WHERE
                      relname = '%s' $$ )).result AS n_live_tup 
                FROM
                  dist_tables_with_indexes 
              )
              SELECT
                tablename,
                CASE sum(idx_scan::int) WHEN 0 THEN
                  'Insufficient data' 
                ELSE
                  (100 * sum(idx_scan::int) / (sum(idx_and_seq_scan::int)))::text 
                END AS percent_of_times_index_used, 
                sum(n_live_tup::int) AS n_live_tup 
              FROM
                dist_indexes 
              GROUP BY
                tablename 
              ORDER BY
                tablename 
            )
            SELECT
              schemaname AS schema,
              relname AS table,
              CASE WHEN (( 
                  SELECT
                    count(*) 
                  FROM
                    pg_dist_index_usage 
                  WHERE
                    tablename::name = relname) > 0)
                THEN ( 
                  SELECT
                    percent_of_times_index_used 
                  FROM
                    pg_dist_index_usage 
                  WHERE
                    tablename::name = relname) 
                ELSE
                  CASE idx_scan 
                    WHEN 0 THEN 'Insufficient data' 
                    ELSE (100 * idx_scan / (seq_scan + idx_scan))::text 
                  END
              END AS percent_of_times_index_used, 
              CASE WHEN (( 
                    SELECT
                      count(*) 
                    FROM
                      pg_dist_index_usage 
                    WHERE
                      tablename::name = relname) > 0)
                THEN ( 
                  SELECT
                    n_live_tup 
                  FROM
                    pg_dist_index_usage 
                  WHERE
                    tablename::name = relname) 
                ELSE
                    n_live_tup 
              END AS estimated_rows 
            FROM
              pg_stat_user_tables 
            ORDER BY
              n_live_tup DESC,
              relname ASC
           SQL
        else
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
         end
      end

      def missing_indexes
        if citus_enabled?
          select_all <<-SQL
            WITH pg_dist_index_usage AS(
              WITH dist_tables_with_indexes (tablename) AS(
                SELECT
                  indrelid 
                FROM
                  pg_index 
                  JOIN
                    pg_dist_partition 
                    ON ( indrelid = logicalrelid) 
              ),
              dist_indexes AS(
                SELECT
                  tablename::regclass,
                  (run_command_on_placements(tablename::regclass, $$ 
                    SELECT
                      idx_scan 
                    FROM
                      pg_stat_user_tables 
                    WHERE
                      relname = '%s' $$ )).result AS idx_scan,
                  (run_command_on_placements(tablename::regclass, $$ 
                    SELECT
                      seq_scan + idx_scan AS idx_and_seq_scan 
                    FROM
                      pg_stat_user_tables 
                    WHERE
                      relname = '%s' $$ )).result AS idx_and_seq_scan,
                  (run_command_on_placements(tablename::regclass, $$ 
                    SELECT
                      n_live_tup 
                    FROM
                      pg_stat_user_tables 
                    WHERE
                      relname = '%s' $$ )).result AS n_live_tup 
                FROM
                  dist_tables_with_indexes 
              )
              SELECT
                tablename,
                sum(idx_scan::int) AS idx_scan,
                sum(idx_and_seq_scan::int) AS idx_and_seq_scan,
                CASE sum(idx_scan::int) WHEN 0 
                  THEN 'Insufficient data' 
                  ELSE (100 * sum(idx_scan::int) / (sum(idx_and_seq_scan::int)))::text 
                END AS percent_of_times_index_used, 
                sum(n_live_tup::int) AS n_live_tup 
              FROM
                dist_indexes 
              GROUP BY
                tablename 
              ORDER BY
                tablename 
            )
            SELECT
              schemaname AS schema,
              relname AS table,
              CASE WHEN (( 
                    SELECT
                      count(*) 
                    FROM
                      pg_dist_index_usage 
                    WHERE
                      tablename::NAME = relname) > 0)
                THEN ( 
                  SELECT
                    percent_of_times_index_used 
                  FROM
                    pg_dist_index_usage 
                  WHERE
                    tablename::NAME = relname) 
                ELSE
                  CASE idx_scan 
                    WHEN 0 THEN 'Insufficient data' 
                    ELSE (100 * idx_scan / (seq_scan + idx_scan))::text 
                  END
              END AS percent_of_times_index_used, 
              CASE WHEN (( 
                    SELECT
                      count(*) 
                    FROM
                      pg_dist_index_usage 
                    WHERE
                      tablename::NAME = relname) > 0)
                THEN ( 
                  SELECT
                    n_live_tup 
                  FROM
                    pg_dist_index_usage 
                  WHERE
                    tablename::NAME = relname) 
                ELSE
                  n_live_tup 
              END AS estimated_rows 
            FROM
              pg_stat_user_tables 
            WHERE(
              ( 
                ((SELECT count(*) FROM pg_dist_index_usage WHERE tablename::NAME = relname) > 0) 
                AND 
                ((SELECT idx_scan FROM pg_dist_index_usage WHERE tablename::NAME = relname) > 0)
                AND 
                ((SELECT 100 * idx_scan / (idx_and_seq_scan) FROM pg_dist_index_usage WHERE tablename::NAME = relname) < 95)
                AND 
                ((SELECT n_live_tup FROM pg_dist_index_usage WHERE tablename::NAME = relname) >= 10000)
              ) 
              OR 
              (
                ((SELECT count(*) FROM pg_dist_index_usage WHERE tablename::NAME = relname) = 0) 
                AND 
                idx_scan > 0 
                AND 
                (100 * idx_scan / (seq_scan + idx_scan)) < 95 
                AND 
                n_live_tup >= 10000 
              )
            )
            ORDER BY
              n_live_tup DESC,
              relname ASC
           SQL
        else
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
         end
      end

      def unused_indexes(max_scans: 50, across: [])
        if citus_enabled?
          result = select_all_size <<-SQL
            WITH indexes (idxname, tablename) AS(
              SELECT
                indexrelid,
                indrelid 
              FROM
                pg_index 
                JOIN
                  pg_dist_partition 
                  ON ( indrelid = logicalrelid) 
            ),
            dist_indexes AS(
              SELECT
                idxname::regclass,
                (run_command_on_placements(tablename::regclass, $$ 
                  SELECT
                    pg_relation_size('$$ || idxname::regclass::text || $$' || replace('%s', '$$ || tablename::regclass::text || $$', '')) $$ )).result 
              FROM
                indexes 
            ),
            pg_dist_index_sizes AS(
              SELECT
                idxname,
                sum(result::bigint) AS idxsize 
              FROM
                dist_indexes 
              GROUP BY
                idxname 
              ORDER BY
                idxname 
            ),
            pg_dist_stat_user_indexes AS 
            (
              SELECT
                idxname::regclass,
                1 AS count,
                (run_command_on_placements(tablename::regclass, $$ 
                  SELECT
                    idx_scan 
                  FROM
                    pg_stat_user_indexes 
                  WHERE
                    indexrelname = '$$ || idxname::regclass::text || $$' || replace('%s', '$$ || tablename::regclass::text || $$', '') $$ )).result AS idx_scan 
              FROM
                indexes 
            ),
            pg_dist_index_scan AS(
              SELECT
                idxname,
                sum(idx_scan::int) AS idx_scan,
                sum(count) AS count 
              FROM
                pg_dist_stat_user_indexes 
              GROUP BY
                idxname 
              ORDER BY
                idxname ASC 
            )
            SELECT
              schemaname AS schema,
              relname AS table,
              indexrelname AS index,
              CASE WHEN (( 
                    SELECT
                      count(*) 
                    FROM
                      pg_dist_partition 
                    WHERE
                      logicalrelid::NAME = relname) > 0)
                THEN( 
                  SELECT
                    idxsize 
                  FROM
                    pg_dist_index_sizes 
                  WHERE
                    idxname::NAME = indexrelname) 
                ELSE
                  pg_relation_size(i.indexrelid) 
              END AS size_bytes,
              CASE WHEN (( 
                    SELECT
                      count(*) 
                    FROM
                      pg_dist_partition 
                    WHERE
                      logicalrelid::NAME = relname) > 0)
                THEN( 
                  SELECT
                    CEILING((idx_scan::decimal) / (count::decimal)) 
                  FROM
                    pg_dist_index_scan 
                  WHERE
                    idxname::name = indexrelname) 
                ELSE
                  idx_scan 
              END AS index_scans 
            FROM
              pg_stat_user_indexes ui 
            INNER JOIN
              pg_index i ON ui.indexrelid = i.indexrelid 
            WHERE
              NOT indisunique 
              AND(
                (
                  ((SELECT count(*) FROM pg_dist_partition WHERE logicalrelid::NAME = relname) > 0) 
                  AND 
                  ((SELECT CEILING((idx_scan::decimal) / (count::decimal)) FROM pg_dist_index_scan WHERE idxname::NAME = indexrelname) <= #{max_scans.to_i} )
                ) 
                OR 
                (
                  ((SELECT count(*) FROM pg_dist_partition WHERE logicalrelid::NAME = relname) = 0) 
                  AND 
                  idx_scan <= #{max_scans.to_i} 
                )
              )
            ORDER BY
              size_bytes DESC,
              relname ASC
          SQL
        else
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
        if citus_enabled?
          execute("SELECT pg_stat_reset() UNION ALL SELECT (run_command_on_workers($cmd$ SELECT pg_stat_reset() $cmd$)).result::void AS pg_stat_reset")
          true
        else
          execute("SELECT pg_stat_reset()")
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
            WITH indexes (idxname,tablename ) AS(
              SELECT
                indexrelid,
                indrelid 
              FROM
                pg_index 
              JOIN
                pg_dist_partition ON ( indrelid = logicalrelid) 
            ),
            pg_dist_index_reltuples AS(
              SELECT
                idxname::regclass,
                (run_command_on_placements(tablename::regclass, $$ 
                  SELECT
                    reltuples 
                  FROM
                    pg_class 
                  WHERE
                    relname = '$$ || idxname::regclass::text || $$' || replace('%s', '$$ || tablename::regclass::text || $$', '') $$ )).result AS reltuples,
                (run_command_on_placements(tablename::regclass, $$ 
                  SELECT
                    relpages 
                  FROM
                    pg_class 
                  WHERE
                    relname = '$$ || idxname::regclass::text || $$' || replace('%s', '$$ || tablename::regclass::text || $$', '') $$ )).result AS relpages 
              FROM
                indexes 
            ),
            pg_dist_index_reltuples_relpages AS 
            (
              SELECT
                idxname,
                sum(reltuples::int) AS reltuples,
                sum(relpages::int) AS relpages 
              FROM
                pg_dist_index_reltuples 
              GROUP BY
                idxname 
              ORDER BY
                idxname ASC 
            ),
            btree_index_atts AS 
            (
              SELECT
                nspname,
                relname,
                CASE WHEN (( 
                      SELECT
                        count(*) 
                      FROM
                        pg_dist_index_reltuples_relpages 
                      WHERE
                        idxname::name = relname) > 0)
                  THEN ( 
                    SELECT
                      reltuples 
                    FROM
                      pg_dist_index_reltuples_relpages 
                    WHERE
                      idxname::name = relname LIMIT 1) 
                  ELSE
                    reltuples 
                END AS reltuples,
                CASE WHEN (( 
                      SELECT
                        count(*) 
                      FROM
                        pg_dist_index_reltuples_relpages 
                      WHERE
                        idxname::name = relname) > 0)
                  THEN ( 
                    SELECT
                      relpages 
                    FROM
                      pg_dist_index_reltuples_relpages 
                    WHERE
                      idxname::name = relname LIMIT 1) 
                  ELSE
                    relpages 
                END AS relpages,
                indrelid,
                relam,
                regexp_split_to_table(indkey::text, ' ')::smallint AS attnum,
                indexrelid AS index_oid 
              FROM
                pg_index 
              JOIN
                pg_class ON pg_class.oid = pg_index.indexrelid 
              JOIN
                pg_namespace ON pg_namespace.oid = pg_class.relnamespace 
              JOIN
                pg_am ON pg_class.relam = pg_am.oid 
              WHERE
                pg_am.amname = 'btree' 
            ),
            pg_dist_stats_regular AS(
              WITH pg_dist_stats_double_json AS(
                SELECT( 
                  SELECT
                    json_agg(row_to_json(f)) 
                  FROM(
                    SELECT
                      result 
                    FROM
                      run_command_on_placements(logicalrelid, $$ 
                        SELECT
                          json_agg(row_to_json(d)) 
                        FROM(
                          (
                            SELECT
                              '$$ || logicalrelid || $$' AS tablename, attname, null_frac, reltuples, avg_width 
                            FROM
                              pg_stats 
                            JOIN
                              pg_class ON tablename = relname 
                            JOIN
                              pg_namespace n ON n.oid = relnamespace 
                            WHERE
                              tablename = '%s' ) 
                            UNION ALL (
                            SELECT
                              'backup' AS tablename, 'backup' AS attname, 0.00 AS null_frac, 1 AS reltuples, 0 AS avg_width) 
                          )
                          d $$)) f) 
                FROM
                  pg_dist_partition 
              ),
              pg_dist_stats_single_json AS 
              (
                SELECT
                  x.result 
                FROM
                  pg_dist_stats_double_json 
                CROSS JOIN
                  json_to_recordset(pg_dist_stats_double_json.json_agg) AS x("result" text) 
              )
              SELECT
                y.tablename,
                y.attname,
                y.null_frac,
                y.reltuples,
                y.avg_width 
              FROM
                pg_dist_stats_single_json 
              CROSS JOIN
                json_to_recordset(pg_dist_stats_single_json.result::json) AS y("tablename" name, "attname" name, "null_frac" real, "reltuples" integer, "avg_width" integer) 
            ),
            pg_dist_stats AS(
              SELECT
                n.nspname AS schemaname,
                tablename,
                attname,
                (sum(null_frac * t.reltuples)) / (sum(t.reltuples)) AS null_frac,
                (sum(avg_width * t.reltuples)) / (sum(t.reltuples)) AS avg_width 
              FROM
                pg_dist_stats_regular t 
              JOIN
                pg_class ON relname = tablename 
              JOIN
                pg_namespace n ON n.oid = relnamespace 
              GROUP BY
                n.nspname,
                tablename,
                attname 
              ORDER BY
                attname ASC 
            ),
            index_item_sizes AS 
            (
              SELECT
                i.nspname,
                i.relname,
                i.reltuples,
                i.relpages,
                i.relam,
                (quote_ident(s.schemaname) || '.' || quote_ident(s.tablename))::regclass AS starelid,
                a.attrelid AS table_oid,
                index_oid,
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
                  (
                    SELECT
                      schemaname,
                      tablename,
                      attname,
                      null_frac,
                      avg_width 
                    FROM
                      pg_stats 
                    WHERE
                      ((SELECT count(*) FROM pg_dist_partition WHERE logicalrelid::name = tablename) = 0)
                    UNION
                    SELECT
                      * 
                    FROM
                      pg_dist_stats 
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
                idxname::regclass,
                1 AS count,
                (run_command_on_placements(tablename::regclass, $$ 
                  SELECT
                    idx_scan 
                  FROM
                    pg_stat_user_indexes 
                  WHERE
                    indexrelname = '$$ || idxname::regclass::text || $$' || replace('%s', '$$ || tablename::regclass::text || $$', '') $$ )).result AS idx_scan 
              FROM
                indexes 
            ),
            pg_dist_index_scan AS 
            (
              SELECT
                idxname,
                sum(idx_scan::int) AS idx_scan,
                sum(count) AS count 
              FROM
                pg_dist_stat_user_indexes 
              GROUP BY
                idxname 
              ORDER BY
                idxname ASC 
            ),
            raw_bloat AS(
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
                CASE WHEN (( 
                      SELECT
                        count(*) 
                      FROM
                        pg_dist_partition 
                      WHERE
                        logicalrelid::oid = sub.table_oid) > 0)
                  THEN
                    citus_relation_size(sub.table_oid) 
                  ELSE
                    pg_relation_size(sub.table_oid) 
                END AS table_bytes, 
                CASE WHEN (( 
                      SELECT
                        count(*) 
                      FROM
                        pg_dist_partition 
                      WHERE
                        logicalrelid::oid = sub.table_oid) > 0)
                  THEN ( 
                    SELECT
                      CEILING((idx_scan::decimal) / (COUNT::decimal)) 
                    FROM
                      pg_dist_index_scan 
                    WHERE
                      idxname::NAME = index_name) 
                  ELSE
                    stat.idx_scan 
                END AS index_scans,
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
        end
      end
    end
  end
end
