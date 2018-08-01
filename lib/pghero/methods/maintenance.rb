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
            WITH dist_transaction_id_danger AS (
              WITH dist_transaction_id_danger_double_json AS (
                SELECT ( 
                  SELECT
                    json_agg(row_to_json(f)) 
                  FROM
                    (
                      SELECT
                        result 
                      FROM
                        run_command_on_workers($$ SELECT
                          json_agg(row_to_json(d)) 
                        FROM
                          (
                            SELECT
                              n.nspname AS schema,
                              c.relname AS TABLE,
                              #{quote(max_value)} - GREATEST(AGE(c.relfrozenxid), AGE(t.relfrozenxid)) AS transactions_left 
                            FROM
                              pg_class c 
                            INNER JOIN
                              pg_catalog.pg_namespace n ON n.oid = c.relnamespace 
                            LEFT JOIN
                              pg_class t ON c.reltoastrelid = t.oid 
                            WHERE
                              c.relkind = 'r' 
                              AND(#{quote(max_value)} - GREATEST(AGE(c.relfrozenxid), AGE(t.relfrozenxid))) < #{quote(threshold)} 
                            ORDER BY 3, 1, 2) d $$)) f)
                ),
              dist_transaction_id_danger_single_json AS (
                SELECT
                  x.result 
                FROM
                  dist_transaction_id_danger_double_json 
                CROSS JOIN
                  json_to_recordset(dist_transaction_id_danger_double_json.json_agg) AS x("result" text) 
              )
              SELECT
                y.schema,
                y.table,
                y.transactions_left 
              FROM
                dist_transaction_id_danger_single_json 
              CROSS JOIN
                json_to_recordset(dist_transaction_id_danger_single_json.result::json) AS y("schema" name, "table" name, "transactions_left" INTEGER) 
              WHERE
                dist_transaction_id_danger_single_json.result != '' 
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
            AND 
              (#{quote(max_value)} - GREATEST(AGE(c.relfrozenxid), AGE(t.relfrozenxid))) < #{quote(threshold)} 
            UNION ALL
              (SELECT * FROM dist_transaction_id_danger) 
            ORDER BY 3, 1, 2
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
                    last_vacuum, last_autovacuum, last_analyze, last_autoanalyze
                  FROM
                    pg_stat_user_tables 
                  WHERE
                    relname = '%s') d $$)).result AS all_info
              FROM
                pg_dist_partition 
                ),
            dist_maintenance_info as (
              SELECT
                logicalrelid,
                x.last_vacuum,
                x.last_autovacuum,
                x.last_analyze,
                x.last_autoanalyze
              FROM
                json_dist_maintenance_info 
              CROSS JOIN
                json_to_recordset(json_dist_maintenance_info.all_info::json) AS x("last_vacuum" timestamp with time zone, "last_autovacuum" timestamp with time zone, "last_analyze" timestamp with time zone, "last_autoanalyze" timestamp with time zone)           
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
                THEN(
                      SELECT
                        MIN(last_vacuum) 
                      FROM
                        dist_maintenance_info 
                      WHERE
                        logicalrelid::name = relname)                  
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
                THEN(
                      SELECT
                        MIN(last_autovacuum) 
                      FROM
                        dist_maintenance_info 
                      WHERE
                        logicalrelid::name = relname)                 
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
                THEN (
                      SELECT
                        MIN(last_analyze) 
                      FROM
                        dist_maintenance_info 
                      WHERE
                        logicalrelid::name = relname)                  
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
                THEN (
                      SELECT
                        MIN(last_autoanalyze) 
                      FROM
                        dist_maintenance_info 
                      WHERE
                        logicalrelid::name = relname)                   
                ELSE
                  last_autoanalyze 
              END AS last_autoanalyze 
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
