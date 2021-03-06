module PgHero
  module Methods
    module Space
      include Citus
      
      def database_size
        if !citus_enabled?
          PgHero.pretty_size select_one("SELECT pg_database_size(current_database())")
        else
          PgHero.pretty_size select_one("SELECT 
                                           sum(size) 
                                         FROM (
                                           SELECT 
                                             pg_database_size(current_database()) AS size 
                                           UNION ALL 
                                           SELECT 
                                             result::bigint AS size 
                                           FROM run_command_on_workers($cmd$ SELECT 
                                                                               pg_database_size(current_database()); 
                                                                       $cmd$)) AS worker_size")
        end
      end

      def node_sizes
        select_all <<-SQL
          SELECT
            'Coordinator' AS name,
            pg_database_size(current_database()) AS size
          UNION ALL
          SELECT
            'Worker Node ' || nodeid AS name,
            result::bigint AS size
          FROM
            (SELECT (run_command_on_workers($$ SELECT pg_database_size(current_database()) $$)).* ) w
          JOIN
            pg_dist_node pn ON (pn.nodename = w.nodename AND pn.nodeport = w.nodeport)
        SQL
      end

      def relation_sizes
        if !citus_enabled?
          select_all_size <<-SQL
            SELECT
              n.nspname AS schema,
              c.relname AS relation,
              CASE WHEN c.relkind = 'r' THEN 'table' ELSE 'index' END AS type,
              pg_table_size(c.oid) AS size_bytes
            FROM
              pg_class c
            LEFT JOIN
              pg_namespace n ON n.oid = c.relnamespace
            WHERE
              n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND n.nspname !~ '^pg_toast'
              AND c.relkind IN ('r', 'i')
            ORDER BY
              pg_table_size(c.oid) DESC,
              2 ASC
          SQL
        else
          select_all_size <<-SQL
            WITH dist_indexes(idxname, tablename) AS (
              SELECT 
                indexrelid,
                indrelid,
                partmethod
              FROM
                pg_index
              JOIN
                pg_dist_partition ON (indrelid = logicalrelid)
            ),
            dist_indexes_shard_sizes AS (
              SELECT
                idxname::regclass,
                (run_command_on_placements(tablename::regclass, $$
                  SELECT
                    pg_table_size(replace('%s','$$ || tablename::regclass::text || $$', '$$ || idxname::regclass::text || $$'))
                $$)).result
              FROM
                dist_indexes
            ),
            dist_indexes_sizes AS (
            SELECT
              idxname,
              sum(result::bigint) AS idxsize
            FROM
              dist_indexes_shard_sizes
            GROUP BY
              idxname
            )
            SELECT
              n.nspname AS schema,
              c.relname AS relation,
              CASE WHEN c.relkind = 'r' THEN 'table' ELSE 'index' END AS type,
              CASE WHEN c.relkind = 'r' THEN
                CASE WHEN EXISTS(SELECT * FROM pg_dist_partition WHERE logicalrelid = c.oid)
                  THEN citus_table_size(c.oid)
                  ELSE pg_table_size(c.oid)
                END
              ELSE
                CASE WHEN EXISTS(SELECT * FROM dist_indexes_sizes WHERE idxname = c.oid)
                  THEN (SELECT idxsize FROM dist_indexes_sizes WHERE idxname = c.oid LIMIT 1)
                  ELSE pg_table_size(c.oid)
                END
              END AS size_bytes,
              CASE WHEN c.relkind = 'r' THEN
                CASE WHEN EXISTS(SELECT * FROM pg_dist_partition WHERE logicalrelid = c.oid AND partmethod != 'n') THEN 'distributed'
                WHEN EXISTS(SELECT * FROM pg_dist_partition WHERE logicalrelid = c.oid AND partmethod = 'n') THEN 'reference'
                ELSE '-'
                END
              ELSE
                CASE WHEN EXISTS(SELECT * FROM dist_indexes WHERE idxname = c.oid AND partmethod != 'n') THEN 'distributed'
                WHEN EXISTS(SELECT * FROM dist_indexes WHERE idxname = c.oid AND partmethod = 'n') THEN 'reference'
                ELSE '-'
                END
              END AS partmethod
            FROM
              pg_class c
            LEFT JOIN
              pg_namespace n ON n.oid = c.relnamespace
            WHERE
              n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND n.nspname !~ '^pg_toast'
              AND c.relkind IN ('r', 'i')
            ORDER BY
              size_bytes DESC,
              2 ASC
          SQL
        end
      end

      def table_sizes
        if !citus_enabled?
          select_all_size <<-SQL
            SELECT
              n.nspname AS schema,
              c.relname AS table,
              pg_total_relation_size(c.oid) AS size_bytes
            FROM
              pg_class c
            LEFT JOIN
              pg_namespace n ON n.oid = c.relnamespace
            WHERE
              n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND n.nspname !~ '^pg_toast'
              AND c.relkind = 'r'
            ORDER BY
              pg_total_relation_size(c.oid) DESC,
              2 ASC          
          SQL
        else
          select_all_size <<-SQL
            SELECT 
              n.nspname AS SCHEMA, 
              c.relname AS table, 
              CASE WHEN EXISTS(SELECT * FROM pg_dist_partition WHERE logicalrelid = c.oid) 
                THEN citus_total_relation_size(c.oid) 
                ELSE pg_total_relation_size(c.oid) 
              END AS size_bytes 
            FROM   
              pg_class c 
            LEFT JOIN 
              pg_namespace n ON n.oid = c.relnamespace 
            WHERE 
              n.nspname NOT IN ('pg_catalog', 'information_schema') 
              AND n.nspname !~ '^pg_toast' 
              AND c.relkind = 'r' 
            ORDER BY 
              size_bytes DESC, 
              2 ASC
          SQL
        end
      end

      def space_growth(days: 7, relation_sizes: nil)
        if space_stats_enabled?
          relation_sizes ||= self.relation_sizes
          sizes = Hash[ relation_sizes.map { |r| [[r[:schema], r[:relation]], r[:size_bytes]] } ]
          start_at = days.days.ago

          stats = select_all_stats <<-SQL
            WITH t AS (
              SELECT
                schema,
                relation,
                array_agg(size ORDER BY captured_at) AS sizes
              FROM
                pghero_space_stats
              WHERE
                database = #{quote(id)}
                AND captured_at >= #{quote(start_at)}
              GROUP BY
                1, 2
            )
            SELECT
              schema,
              relation,
              sizes[1] AS size_bytes
            FROM
              t
            ORDER BY
              1, 2
          SQL

          stats.each do |r|
            relation = [r[:schema], r[:relation]]
            if sizes[relation]
              r[:growth_bytes] = sizes[relation] - r[:size_bytes]
            end
            r.delete(:size_bytes)
          end
          stats
        else
          raise NotEnabled, "Space stats not enabled"
        end
      end

      def relation_space_stats(relation, schema: "public")
        if space_stats_enabled?
          relation_sizes ||= self.relation_sizes
          sizes = Hash[ relation_sizes.map { |r| [[r[:schema], r[:relation]], r[:size_bytes]] } ]
          start_at = 30.days.ago

          stats = select_all_stats <<-SQL
            SELECT
              captured_at,
              size AS size_bytes
            FROM
              pghero_space_stats
            WHERE
              database = #{quote(id)}
              AND captured_at >= #{quote(start_at)}
              AND schema = #{quote(schema)}
              AND relation = #{quote(relation)}
            ORDER BY
              1 ASC
          SQL

          stats << {
            captured_at: Time.now,
            size_bytes: sizes[[schema, relation]].to_i
          }
        else
          raise NotEnabled, "Space stats not enabled"
        end
      end

      def capture_space_stats
        now = Time.now
        columns = %w(database schema relation size captured_at)
        values = []
        relation_sizes.each do |rs|
          values << [id, rs[:schema], rs[:relation], rs[:size_bytes].to_i, now]
        end
        insert_stats("pghero_space_stats", columns, values) if values.any?
      end

      def space_stats_enabled?
        table_exists?("pghero_space_stats")
      end
    end
  end
end
