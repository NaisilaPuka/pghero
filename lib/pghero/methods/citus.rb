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
          WITH nodes_sizes AS (
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
            nodes_sizes s,
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
    end
  end
end
