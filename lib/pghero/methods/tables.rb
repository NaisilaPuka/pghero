module PgHero
  module Methods
    module Tables
      include Citus

      def table_hit_rate
        if !citus_enabled?
          select_one(<<-SQL
            SELECT
              sum(heap_blks_hit) / nullif(sum(heap_blks_hit) + sum(heap_blks_read), 0) AS rate
            FROM
              pg_statio_user_tables
          SQL
          )
        else
          select_one(<<-SQL
            WITH worker_heap_blks_stats_json AS (
              SELECT
                result::json
              FROM
                run_command_on_workers($$
                SELECT
                  json_agg(row_to_json(worker_heap_blks_stats))
                FROM
                  (
                    SELECT
                      sum(heap_blks_hit) AS heap_blks_hit,
                      sum(heap_blks_read) AS heap_blks_read
                    FROM
                      pg_statio_user_tables
                  )
                  worker_heap_blks_stats $$)
            )
            SELECT
              sum(heap_blks_hit) / nullif(sum(heap_blks_hit) + sum(heap_blks_read), 0) AS rate
            FROM
              (
                SELECT
                  heap_blks_hit,
                  heap_blks_read
                FROM
                  pg_statio_user_tables
                UNION ALL
                SELECT
                  (json_array_elements(result)->>'heap_blks_hit')::bigint AS heap_blks_hit,
                  (json_array_elements(result)->>'heap_blks_read')::bigint AS heap_blks_read
                FROM
                  worker_heap_blks_stats_json
              ) AS cluster_heap_blks_stats
          SQL
          )
        end
      end

      def table_caching
        if !citus_enabled?
          select_all <<-SQL
            SELECT
              schemaname AS schema,
              relname AS table,
              CASE WHEN heap_blks_hit + heap_blks_read = 0 THEN
                0
              ELSE
                ROUND(1.0 * heap_blks_hit / (heap_blks_hit + heap_blks_read), 2)
              END AS hit_rate
            FROM
              pg_statio_user_tables
            ORDER BY
              2 DESC, 1
          SQL
        else
          select_all <<-SQL
            WITH placements_heap_blks_stats AS (
              SELECT
                logicalrelid,
                (run_command_on_placements(logicalrelid, $$
                  SELECT
                    heap_blks_hit AS hit
                  FROM
                    pg_statio_user_tables
                  WHERE
                    relid = '%s'::regclass::oid $$ )).result::bigint AS heap_blks_hit,
                (run_command_on_placements(logicalrelid, $$
                  SELECT
                    heap_blks_read
                  FROM
                    pg_statio_user_tables
                  WHERE
                    relid = '%s'::regclass::oid $$ )).result::bigint AS heap_blks_read
              FROM
                pg_dist_partition
            )
            SELECT
              schemaname AS schema,
              relname AS table,
              CASE WHEN heap_blks_hit + heap_blks_read = 0 THEN
                0
              ELSE
                ROUND(1.0 * heap_blks_hit / (heap_blks_hit + heap_blks_read), 2)
              END AS hit_rate
            FROM (
              SELECT
                n.nspname AS schemaname,
                c.relname,
                sum(p.heap_blks_hit) AS heap_blks_hit,
                sum(p.heap_blks_read) AS heap_blks_read
              FROM
                placements_heap_blks_stats p
                JOIN pg_class c ON p.logicalrelid::oid = c.oid
                LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
              GROUP BY
                2, 1
              UNION ALL
              SELECT
                schemaname,
                relname,
                heap_blks_hit,
                heap_blks_read
              FROM
                pg_statio_user_tables s
              WHERE
                NOT EXISTS(SELECT * FROM pg_dist_partition WHERE logicalrelid = (schemaname || '.' || relname)::regclass)
            ) all_stats
            ORDER BY
              2 DESC, 1
          SQL
        end
      end

      def unused_tables
        if !citus_enabled?
          select_all <<-SQL
            SELECT
              schemaname AS schema,
              relname AS table,
              n_live_tup AS estimated_rows
            FROM
              pg_stat_user_tables
            WHERE
              idx_scan = 0
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
            placements_stats AS (
              SELECT
                tablename,
                (run_command_on_placements(tablename, $$
                  SELECT
                    idx_scan
                  FROM
                    pg_stat_user_tables
                  WHERE
                    relid = '%s'::regclass::oid $$ )).result::bigint AS idx_scan,
                (run_command_on_placements(tablename, $$
                  SELECT
                    n_live_tup
                  FROM
                    pg_stat_user_tables
                  WHERE
                    relid = '%s'::regclass::oid $$ )).result::bigint AS n_live_tup
              FROM
                dist_tables_with_indexes
            )
            SELECT
              schemaname AS schema,
              relname AS table,
              n_live_tup AS estimated_rows
            FROM (
              SELECT
                n.nspname AS schemaname,
                c.relname,
                sum(idx_scan) AS idx_scan,
                sum(n_live_tup) AS n_live_tup
              FROM
                placements_stats p
                JOIN pg_class c ON p.tablename::oid = c.oid
                LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
              GROUP BY
                2, 1
              UNION ALL
              SELECT
                schemaname,
                relname,
                idx_scan,
                n_live_tup
              FROM
                pg_stat_user_tables s
              WHERE
                NOT EXISTS(SELECT * FROM dist_tables_with_indexes WHERE tablename = (schemaname || '.' || relname)::regclass)
            ) all_stats
            WHERE
              idx_scan = 0
            ORDER BY
              n_live_tup DESC,
              relname ASC
          SQL
        end
      end

      def table_stats(schema: nil, table: nil)
        if !citus_enabled?
          select_all <<-SQL
            SELECT
              nspname AS schema,
              relname AS table,
              reltuples::bigint AS estimated_rows,
              pg_total_relation_size(pg_class.oid) AS size_bytes
            FROM
              pg_class
            INNER JOIN
              pg_namespace ON pg_namespace.oid = pg_class.relnamespace
            WHERE
              relkind = 'r'
              #{schema ? "AND nspname = #{quote(schema)}" : nil}
              #{table ? "AND relname IN (#{Array(table).map { |t| quote(t) }.join(", ")})" : nil}
            ORDER BY
              1, 2
          SQL
        else
          select_all <<-SQL
            WITH placements_reltuples AS (
              SELECT
                logicalrelid,
                (run_command_on_placements(logicalrelid, $$
                  SELECT
                    reltuples
                  FROM
                    pg_class
                  WHERE
                    oid = '%s'::regclass::oid $$ )).result::real AS reltuples
              FROM
                pg_dist_partition
            )
            SELECT
              nspname AS schema,
              relname AS table,
              reltuples::bigint AS estimated_rows,
              size_bytes
            FROM (
              SELECT
                n.nspname,
                c.relname,
                sum(p.reltuples) AS reltuples,
                citus_total_relation_size(c.oid) AS size_bytes,
                c.relkind
              FROM
                placements_reltuples p
                JOIN pg_class c ON p.logicalrelid::oid = c.oid
                LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
              GROUP BY
                2, 1, 4, 5
              UNION ALL
              SELECT
                nspname,
                relname,
                reltuples,
                pg_total_relation_size(pg_class.oid) AS size_bytes,
                relkind
              FROM
                pg_class
              INNER JOIN
                pg_namespace ON pg_namespace.oid = pg_class.relnamespace
              WHERE
                NOT EXISTS(SELECT * FROM pg_dist_partition WHERE logicalrelid = (nspname || '.' || relname)::regclass)
            ) all_stats
            WHERE
              relkind = 'r'
              #{schema ? "AND nspname = #{quote(schema)}" : nil}
              #{table ? "AND relname IN (#{Array(table).map { |t| quote(t) }.join(", ")})" : nil}
            ORDER BY
              1, 2
          SQL
        end
      end
    end
  end
end
