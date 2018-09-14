module PgHero
  module Methods
    module Citus
      # It will only cache citus_enabled if it is true once.
      def citus_enabled?
        @citus_enabled ||= citus_readable?
      end

      def citus_readable?
        select_one("SELECT EXISTS(SELECT * FROM pg_extension WHERE extname = 'citus')")
      end

      def citus_worker_count
        @citus_worker_count = select_one("SELECT COUNT(*) FROM master_get_active_worker_nodes()")
      end

      def citus_version
        @citus_version ||= select_one("SHOW citus.version")
      end

      def nodes_info
        select_all <<-SQL
          WITH node_sizes AS (
            SELECT
              nodename,
              nodeport,
              result
            FROM
              run_command_on_workers($$ SELECT pg_database_size(current_database()) $$)
          ),
          dist_nodes AS (
            SELECT
              nodeid,
              nodename,
              nodeport,
              noderole,
              isactive,
              count(*) AS shard_count
            FROM
              pg_dist_placement p,
              pg_dist_node n
            WHERE
              p.groupid = n.groupid
            GROUP BY
              1, 2, 3, 4, 5
          )
          SELECT
            nodeid AS id,
            n.nodename AS name,
            n.nodeport AS port,
            noderole AS role,
            isactive AS status,
            shard_count,
            result AS size
          FROM
            node_sizes s,
            dist_nodes n
          WHERE
            s.nodename = n.nodename AND s.nodeport = n.nodeport
          ORDER BY
            1
        SQL
      end

      def colocated_shard_sizes
        select_all <<-SQL
          WITH shard_sizes AS (
            SELECT
              shardid,
              result::bigint AS size
            FROM (
              SELECT 
                (run_command_on_shards(logicalrelid, $$ SELECT pg_total_relation_size('%s') $$)).*
              FROM
                pg_dist_partition
              WHERE
                partmethod = 'h'
            ) hash_dist_shard_sizes
          )
          SELECT
            pn.nodeid,
            array_agg(ps.logicalrelid || '_' || ps.shardid) AS shard_group,
            array_agg(size) AS each_shard_size,
            sum(size) AS colocated_shards_size
          FROM
            shard_sizes ss,
            pg_dist_partition pp,
            pg_dist_shard ps,
            pg_dist_placement ppl,
            pg_dist_node pn
          WHERE
            ss.shardid = ps.shardid
            AND ps.shardid = ppl.shardid
            AND pp.logicalrelid = ps.logicalrelid
            AND ppl.groupid = pn.groupid
          GROUP BY
            shardmaxvalue,
            shardminvalue,
            1, pp.colocationid
          ORDER BY
            1, 4 DESC
        SQL
      end

      def landlord_available?
        select_all("SELECT * FROM citus_stat_statements LIMIT 1")
        true
      rescue ActiveRecord::StatementInvalid
        false
      end

      def landlord_stats
        select_all <<-SQL
          SELECT
            queryid,
            left(query, 10000) AS query,
            executor,
            CASE WHEN partition_key = '' IS NOT FALSE THEN '-' ELSE partition_key END AS partition_key,
            calls
          FROM
            citus_stat_statements
          INNER JOIN
            pg_database ON pg_database.oid = citus_stat_statements.dbid
          WHERE
            pg_database.datname = current_database()
          ORDER BY
            5 DESC
        SQL
      end

      def reset_landlord_stats(raise_errors: false)
        execute("SELECT citus_stat_statements_reset()")
        true
      rescue ActiveRecord::StatementInvalid => e
        raise e if raise_errors
        false
      end

      def distributed_tables
        select_all <<-SQL
          SELECT
            logicalrelid,
            schemaname AS schema,
            tablename AS dist_table_name,
            column_to_column_name(logicalrelid, partkey) AS partition_column,
            citus_relation_size(logicalrelid) AS size_bytes
          FROM
            pg_dist_partition
          JOIN
            pg_tables
            ON logicalrelid = (schemaname || '.' || tablename)::regclass
          WHERE
            partmethod != 'n'
          ORDER BY
            5 DESC
        SQL
      end

      def shard_data_distribution(logicalrelid, partition_column)
        select_all <<-SQL
          WITH tenants AS (
            SELECT
              shardid,
              result::int AS tenant_count
            FROM
              run_command_on_shards(#{quote(logicalrelid)}, $$
                SELECT
                  count(distinct #{quote_ident(partition_column)})
                FROM
                  %s $$)
          ),
          sizes AS (
            SELECT
              shardid,
              result::bigint AS size_bytes
            FROM
              run_command_on_shards(#{quote(logicalrelid)}, $$
                SELECT
                  pg_table_size('%s') $$)
          )
          SELECT
            nodeid,
            tenants.shardid AS id,
            tenant_count,
            size_bytes
          FROM
            tenants
          JOIN
            sizes ON tenants.shardid = sizes.shardid
          JOIN
            pg_dist_placement pdp ON pdp.shardid = tenants.shardid
          JOIN
            pg_dist_node pdn ON pdp.groupid = pdn.groupid
          ORDER BY
            1, 4 DESC
        SQL
      end

      def dist_tables_extended
        select_all <<-SQL
          SELECT
            colocationid AS coloc_id,
            schemaname AS schema,
            tablename AS dist_table_name,
            CASE WHEN partmethod != 'n' THEN 'distributed' ELSE 'reference' END AS partmethod,
            CASE WHEN partmethod != 'n' THEN column_to_column_name(logicalrelid, partkey) ELSE '-' END AS partition_column,
            (SELECT count(*) FROM run_command_on_shards(logicalrelid, 'SELECT 1')) AS shard_count,
            citus_total_relation_size(logicalrelid) AS part_size
          FROM
            pg_dist_partition
          JOIN
            pg_tables
            ON logicalrelid = (schemaname || '.' || tablename)::regclass
          ORDER BY
            1, 3, 2 DESC
        SQL
      end

      def citus_settings
        names =
            %i(
                  citus.max_worker_nodes_tracked
                  citus.use_secondary_nodes
                  citus.cluster_name
                  citus.enable_version_checks
                  citus.log_distributed_deadlock_detection
                  citus.distributed_deadlock_detection_factor
                  citus.multi_shard_commit_protocol
                  citus.shard_replication_factor
                  citus.shard_count
                  citus.shard_max_size
                  citus.limit_clause_row_fetch_count
                  citus.count_distinct_error_rate
                  citus.task_assignment_policy
                  citus.binary_worker_copy_format
                  citus.binary_master_copy_format
                  citus.max_intermediate_result_size
                  citus.enable_ddl_propagation
                  citus.all_modifications_commutative
                  citus.max_task_string_size
                  citus.remote_task_check_interval
                  citus.task_executor_type
                  citus.multi_task_query_log_level
                  citus.enable_repartition_joins
                  citus.task_tracker_delay
                  citus.max_tracked_tasks_per_node
                  citus.max_assign_task_batch_size
                  citus.max_running_tasks_per_node
                  citus.partition_buffer_size
                  citus.explain_all_tasks
            )
         citus_fetch_settings(names)
      end

      private

      def citus_fetch_settings(names)
        Hash[names.map { |name| [name[6..-1], select_one("SHOW #{name}")] }]
      end
    end
  end
end
