module PgHero
  module Methods
    module Maintenance
      include Citus
      
      # https://www.postgresql.org/docs/9.1/static/routine-vacuuming.html#VACUUM-FOR-WRAPAROUND
      # "the system will shut down and refuse to start any new transactions
      # once there are fewer than 1 million transactions left until wraparound"
      # warn when 10,000,000 transactions left
      def transaction_id_danger(threshold: 10000000, max_value: 2146483648)
        max_value = max_value.to_i
        threshold = threshold.to_i

        if !citus_enabled?
          select_all <<-SQL
            SELECT
              n.nspname AS schema,
              c.relname AS table,
              #{quote(max_value)} - GREATEST(AGE(c.relfrozenxid), AGE(t.relfrozenxid)) AS transactions_left
            FROM
              pg_class c
            INNER JOIN
              pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN
              pg_class t ON c.reltoastrelid = t.oid
            WHERE
              c.relkind = 'r'
              AND (#{quote(max_value)} - GREATEST(AGE(c.relfrozenxid), AGE(t.relfrozenxid))) < #{quote(threshold)}
            ORDER BY
             3, 1, 2
          SQL
        else
          select_all <<-SQL
            WITH dist_transaction_id_danger_double_json AS (
              SELECT (
                SELECT
                  json_agg(row_to_json(f))
                FROM (
                  SELECT
                    result
                  FROM
                    run_command_on_workers($$
                      SELECT
                        json_agg(row_to_json(d))
                      FROM (
                        SELECT
                          n.nspname AS schema,
                          c.relname AS table,
                          #{quote(max_value)} - GREATEST(AGE(c.relfrozenxid), AGE(t.relfrozenxid)) AS transactions_left
                        FROM
                          pg_class c
                        INNER JOIN
                          pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        LEFT JOIN
                          pg_class t ON c.reltoastrelid = t.oid
                        WHERE
                          c.relkind = 'r'
                          AND (#{quote(max_value)} - GREATEST(AGE(c.relfrozenxid), AGE(t.relfrozenxid))) < #{quote(threshold)}
                        ORDER BY 3, 1, 2
                      ) d $$)
                ) f
              )
            ),
            dist_transaction_id_danger_single_json AS (
              SELECT
                (json_array_elements(json_agg)->>'result') AS result
              FROM
                dist_transaction_id_danger_double_json
            )
            SELECT
              n.nspname AS schema,
              c.relname AS table,
              #{quote(max_value)} - GREATEST(AGE(c.relfrozenxid), AGE(t.relfrozenxid)) AS transactions_left
            FROM
              pg_class c
            INNER JOIN
              pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN
              pg_class t ON c.reltoastrelid = t.oid
            WHERE
              c.relkind = 'r'
              AND (#{quote(max_value)} - GREATEST(AGE(c.relfrozenxid), AGE(t.relfrozenxid))) < #{quote(threshold)}
            UNION ALL
            SELECT
              (json_array_elements(result::json)->>'schema')::name AS schema,
              (json_array_elements(result::json)->>'table')::name AS table,
              (json_array_elements(result::json)->>'transactions_left')::integer AS transactions_left
            FROM
              dist_transaction_id_danger_single_json
            WHERE
              result != ''
            ORDER BY 
              3, 1, 2
          SQL
        end
      end

      def autovacuum_danger
        max_value = select_one("SHOW autovacuum_freeze_max_age").to_i
        transaction_id_danger(threshold: 2000000, max_value: max_value)
      end

      def vacuum_progress
        if server_version_num >= 90600
          select_all <<-SQL
            SELECT
              pid,
              phase
            FROM
              pg_stat_progress_vacuum
            WHERE
              datname = current_database()
          SQL
        else
          []
        end
      end

      def maintenance_info
        if !citus_enabled?
          select_all <<-SQL
            SELECT
              schemaname AS schema,
              relname AS table,
              last_vacuum,
              last_autovacuum,
              last_analyze,
              last_autoanalyze
            FROM
              pg_stat_user_tables
            ORDER BY
              1, 2
          SQL
        else
          select_all <<-SQL
            WITH json_dist_maintenance_info AS (
              SELECT
                logicalrelid,
                (run_command_on_placements(logicalrelid, $$ SELECT json_agg(row_to_json(d)) FROM ( SELECT
                    schemaname, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze
                  FROM
                    pg_stat_user_tables
                  WHERE
                    relid = '%s'::regclass::oid) d $$)).result::json AS all_info
              FROM
                pg_dist_partition
                ),
            dist_maintenance_info as (
              SELECT
                logicalrelid,
                (json_array_elements(all_info)->>'schemaname')::name AS schemaname,
                (json_array_elements(all_info)->>'last_vacuum')::timestamp with time zone AS last_vacuum,
                (json_array_elements(all_info)->>'last_autovacuum')::timestamp with time zone AS last_autovacuum,
                (json_array_elements(all_info)->>'last_analyze')::timestamp with time zone AS last_analyze,
                (json_array_elements(all_info)->>'last_autoanalyze')::timestamp with time zone AS last_autoanalyze
              FROM
                json_dist_maintenance_info
              )
            SELECT
              schemaname AS schema,
              relname AS table,
              last_vacuum,
              last_autovacuum,
              last_analyze,
              last_autoanalyze
            FROM (
              SELECT
                schemaname,
                relname,
                min(last_vacuum) AS last_vacuum,
                min(last_autovacuum) AS last_autovacuum,
                min(last_analyze) AS last_analyze,
                min(last_autoanalyze) AS last_autoanalyze
              FROM
                dist_maintenance_info
              JOIN
                pg_class c ON logicalrelid::oid = c.oid
              GROUP BY
                logicalrelid,
                relname,
                schemaname
              UNION ALL
              SELECT
                schemaname,
                relname,
                last_vacuum,
                last_autovacuum,
                last_analyze,
                last_autoanalyze
              FROM
                pg_stat_user_tables
              WHERE
                NOT EXISTS(SELECT * FROM pg_dist_partition WHERE logicalrelid = (schemaname || '.' || relname)::regclass)
            ) all_stats
            ORDER BY
              1, 2
          SQL
        end
      end

      def analyze(table, verbose: false)
        execute "ANALYZE #{verbose ? "VERBOSE " : ""}#{quote_table_name(table)}"
        true
      end

      def analyze_tables(verbose: false, min_size: nil, tables: nil)
        tables = table_stats(table: tables).reject { |s| %w(information_schema pg_catalog).include?(s[:schema]) }
        tables = tables.select { |s| s[:size_bytes] > min_size } if min_size
        tables.map { |s| s.slice(:schema, :table) }.each do |stats|
          begin
            with_transaction(lock_timeout: 5000, statement_timeout: 120000) do
              analyze "#{stats[:schema]}.#{stats[:table]}", verbose: verbose
            end
            success = true
          rescue ActiveRecord::StatementInvalid => e
            $stderr.puts e.message
            success = false
          end
          stats[:success] = success
        end
      end
    end
  end
end
