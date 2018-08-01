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
        if citus_enabled?
          select_all <<-SQL
            WITH dist_maintenance_info AS (
              SELECT
                logicalrelid,
                (run_command_on_placements(logicalrelid, $$ 
                  SELECT
                    last_vacuum 
                  FROM
                    pg_stat_user_tables 
                  WHERE
                    relname = '%s' $$ )).RESULT AS last_vacuum,
                (run_command_on_placements(logicalrelid, $$ 
                  SELECT
                    last_autovacuum 
                  FROM
                    pg_stat_user_tables 
                  WHERE
                    relname = '%s' $$ )).RESULT AS last_autovacuum,
                (run_command_on_placements(logicalrelid, $$ 
                  SELECT
                    last_analyze 
                  FROM
                    pg_stat_user_tables 
                  WHERE
                    relname = '%s' $$ )).RESULT AS last_analyze,
                (run_command_on_placements(logicalrelid, $$ 
                  SELECT
                    last_autoanalyze 
                  FROM
                    pg_stat_user_tables 
                  WHERE
                    relname = '%s' $$ )).RESULT AS last_autoanalyze 
              FROM
                pg_dist_partition 
                )
            SELECT
              schemaname AS schema,
              relname AS table,
              CASE WHEN ((
                    SELECT
                      COUNT(*) 
                    FROM
                      pg_dist_partition 
                    WHERE
                      logicalrelid::name = relname) > 0)
                THEN
                  CASE WHEN ((
                        SELECT
                          last_vacuum 
                        FROM
                          dist_maintenance_info 
                        WHERE
                          logicalrelid::name = relname LIMIT 1) = '')
                    THEN 
                      NULL::timestamp with time zone 
                    ELSE (
                      SELECT
                        last_vacuum::timestamp with time zone 
                      FROM
                        dist_maintenance_info 
                      WHERE
                        logicalrelid::name = relname LIMIT 1) 
                  END
                ELSE
                  last_vacuum 
              END AS last_vacuum,
              CASE WHEN ((
                    SELECT
                      COUNT(*) 
                    FROM
                      pg_dist_partition 
                    WHERE
                      logicalrelid::name = relname) > 0)
                THEN
                  CASE WHEN ((
                        SELECT
                          last_autovacuum 
                        FROM
                          dist_maintenance_info 
                        WHERE
                          logicalrelid::name = relname LIMIT 1) = '')
                    THEN
                      NULL::timestamp with time zone 
                    ELSE (
                      SELECT
                        MIN(last_autovacuum::timestamp with time zone) 
                      FROM
                        dist_maintenance_info 
                      WHERE
                        logicalrelid::name = relname) 
                  END
                ELSE
                  last_autovacuum 
              END AS last_autovacuum,
              CASE WHEN ((
                    SELECT
                      COUNT(*) 
                    FROM
                      pg_dist_partition 
                    WHERE
                      logicalrelid::name = relname) > 0)
                THEN
                  CASE WHEN
                      ((
                        SELECT
                          last_analyze 
                        FROM
                          dist_maintenance_info 
                        WHERE
                          logicalrelid::name = relname LIMIT 1) = '')
                    THEN
                      NULL::timestamp with time zone 
                    ELSE (
                      SELECT
                        last_analyze::timestamp with time zone 
                      FROM
                        dist_maintenance_info 
                      WHERE
                        logicalrelid::name = relname LIMIT 1) 
                  END
                ELSE
                  last_analyze 
              END AS last_analyze,
              CASE WHEN ((
                    SELECT
                      COUNT(*) 
                    FROM
                      pg_dist_partition 
                    WHERE
                      logicalrelid::name = relname) > 0)
                THEN
                  CASE WHEN ((
                        SELECT
                          last_autoanalyze 
                        FROM
                          dist_maintenance_info 
                        WHERE
                          logicalrelid::name = relname LIMIT 1) = '')
                    THEN
                      NULL::timestamp with time zone 
                    ELSE (
                      SELECT
                        MIN(last_autoanalyze::timestamp with time zone) 
                      FROM
                        dist_maintenance_info 
                      WHERE
                        logicalrelid::name = relname) 
                  END
                ELSE
                  last_autoanalyze 
              END AS last_autoanalyze 
            FROM
              pg_stat_user_tables 
            ORDER BY
              1, 2                          
          SQL
        else
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
