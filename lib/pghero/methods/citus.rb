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
    end
  end
end
