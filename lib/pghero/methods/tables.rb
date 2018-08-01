module PgHero
  module Methods
    module Tables
      include Citus

      def table_hit_rate
        if citus_enabled?
          select_one(<<-SQL
            WITH worker_index_stats AS (
              SELECT
                RESULT 
              FROM
                run_command_on_workers($cmd$ 
                SELECT
                  json_agg(row_to_json(d)) 
                FROM
                  (
                    SELECT
                      sum(heap_blks_hit) AS hit,
                      nullif(sum(heap_blks_hit + heap_blks_read), 0) AS hit_and_read 
                    FROM
                      pg_statio_user_tables 
                  )
                  d $cmd$) 
            )
            SELECT
              sum(hit) / sum(hit_and_read) AS rate 
            FROM
              (
                SELECT
                  sum(heap_blks_hit) AS hit,
                  nullif(sum(heap_blks_hit + heap_blks_read), 0) AS hit_and_read 
                FROM
                  pg_statio_user_tables 
                UNION ALL
                SELECT
                  x.hit,
                  x.hit_and_read 
                FROM
                  worker_index_stats 
                CROSS JOIN
                  json_to_recordset(worker_index_stats.result::json) AS x("hit" int, "hit_and_read" int) 
              )
              AS cluster_table_hit_rate
          SQL
          )
        else
          select_one(<<-SQL
            SELECT
              sum(heap_blks_hit) / nullif(sum(heap_blks_hit) + sum(heap_blks_read), 0) AS rate
            FROM
              pg_statio_user_tables
          SQL
          )
        end
      end

      def table_caching
        if citus_enabled?
          select_all <<-SQL
            WITH pg_dist_table_caching AS (
              WITH dist_tables AS (
                SELECT
                  logicalrelid,
                  (run_command_on_placements(logicalrelid, $$ 
                    SELECT
                      heap_blks_hit AS hit 
                    FROM
                      pg_statio_user_tables 
                    WHERE
                      relname = '%s' $$ )).RESULT AS hit,
                  (run_command_on_placements(logicalrelid, $$ 
                    SELECT
                      heap_blks_hit + heap_blks_read AS hit_and_read 
                    FROM
                      pg_statio_user_tables 
                    WHERE
                      relname = '%s' $$ )).RESULT AS hit_and_read 
                FROM
                  pg_dist_partition 
              )
              SELECT
                logicalrelid,
                CASE WHEN sum(hit_and_read::decimal) = 0 THEN 0 ELSE ROUND(1.0 * sum(hit::decimal) / sum(hit_and_read::decimal), 2) 
                END AS hit_rate 
              FROM
                dist_tables 
              GROUP BY
                logicalrelid 
              ORDER BY
                logicalrelid 
            )
            SELECT
              schemaname AS SCHEMA,
              relname AS TABLE,
              CASE WHEN (( 
                    SELECT
                      COUNT(*) 
                    FROM
                      pg_dist_partition 
                    WHERE
                      logicalrelid::name = relname) > 0)
                THEN ( 
                  SELECT
                    hit_rate 
                  FROM
                    pg_dist_table_caching 
                  WHERE
                    logicalrelid::name = relname LIMIT 1) 
                  ELSE
                    CASE
                      WHEN heap_blks_hit + heap_blks_read = 0 THEN 
                        0 
                      ELSE
                        ROUND(1.0 * heap_blks_hit / (heap_blks_hit + heap_blks_read), 2) 
                    END
              END AS hit_rate 
            FROM
              pg_statio_user_tables 
            ORDER BY
              2 DESC, 1
          SQL
        else
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
        end
      end

      def unused_tables
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
              dist_tables AS(
                SELECT
                  tablename::regclass,
                  (run_command_on_placements(tablename::regclass, $$ 
                    SELECT
                      idx_scan 
                    FROM
                      pg_stat_user_tables 
                    WHERE
                      relname = '%s' $$ )).RESULT AS idx_scan,
                  (run_command_on_placements(tablename::regclass, $$ 
                    SELECT
                      n_live_tup 
                    FROM
                      pg_stat_user_tables 
                    WHERE
                      relname = '%s' $$ )).RESULT AS n_live_tup 
                FROM
                  dist_tables_with_indexes 
              )
              SELECT
                tablename,
                sum(idx_scan::int) AS idx_scan,
                sum(n_live_tup::int) AS n_live_tup 
              FROM
                dist_tables 
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
                      COUNT(*) 
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
            WHERE (
              (
                ((SELECT count(*) FROM pg_dist_index_usage WHERE tablename::NAME = relname) > 0) 
                AND 
                ((SELECT idx_scan FROM pg_dist_index_usage WHERE tablename::NAME = relname) = 0)
              ) 
              OR 
              (
                ((SELECT count(*) FROM pg_dist_index_usage WHERE tablename::NAME = relname) = 0) 
                  AND 
                idx_scan = 0 
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
              n_live_tup AS estimated_rows
            FROM
              pg_stat_user_tables
            WHERE
              idx_scan = 0
            ORDER BY
              n_live_tup DESC,
              relname ASC
           SQL
         end
      end

      def table_stats(schema: nil, table: nil)
        if citus_enabled?
          select_all <<-SQL
            WITH pg_dist_table_reltuples AS(
              SELECT
                logicalrelid,
                (run_command_on_placements(logicalrelid, $$ 
                  SELECT
                    reltuples 
                  FROM
                    pg_class 
                  WHERE
                    relname = '%s' $$ )).RESULT AS reltuples 
              FROM
                pg_dist_partition 
            ),
            pg_dist_reltuples AS(
              SELECT
                logicalrelid,
                sum(reltuples::int) AS reltuples 
              FROM
                pg_dist_table_reltuples 
              GROUP BY
                logicalrelid 
              ORDER BY
                logicalrelid ASC 
            )
            SELECT
              nspname AS schema,
              relname AS TABLE,
              CASE WHEN ((
                    SELECT
                      count(*) 
                    FROM
                      pg_dist_partition 
                    WHERE
                      logicalrelid::name = relname) > 0)
                THEN (
                  SELECT
                    reltuples::bigint 
                  FROM
                    pg_dist_reltuples 
                  WHERE
                    logicalrelid::name = relname) 
                ELSE
                    reltuples::bigint 
              END AS estimated_rows,
              CASE WHEN ((
                    SELECT
                      count(*) 
                    FROM
                      pg_dist_partition 
                    WHERE
                      logicalrelid::name = relname) > 0)
                THEN
                  citus_total_relation_size(pg_class.oid) 
                ELSE
                  pg_total_relation_size(pg_class.oid) 
              END AS size_bytes 
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
        end
      end
    end
  end
end
