require "active_record/fax/version"

module ActiveRecord
  module Fax
    # This service works on the assumption that the databases configured in
    # config/database.yml are *local* unless they have the "server" keyword, in
    # that case they are *remote*. e.g. the following configuration is local
    #
    # development:
    #   adapter: mysql2
    #   encoding: utf8
    #   pool: 5
    #   username: test
    #   password: test
    #   database: development
    #   socket: /tmp/mysql.sock
    #
    # while this is remote because it has the server keyword
    #
    # production:
    #   server: your.server.somewhere
    #   server_username: your_user
    #   adapter: mysql2
    #   encoding: utf8
    #   pool: 5
    #   username: test
    #   password: test
    #   database: production
    #   socket: /tmp/mysql.sock

    class Source < ActiveRecord::Base
    end

    class Destination < ActiveRecord::Base
    end

    def self.reload!
      @configs = Hashie::Mash.new(YAML.load_file(File.join(Rails.application.root, 'config', 'database.yml')))
    end

    def self.copy(source_db, dest_db)
      reload!
      src = config(source_db, :source)
      dst = config(dest_db, :destination)
      %x{ssh #{src.server_username}@#{src.server} 'mysqldump --skip-lock-tables --single-transaction -h#{src.host} -u#{src.username} -p"#{src.password}" #{src.database} | gzip -9' | gzip -cd | mysql -u#{dst.username} -p#{dst.password} #{dst.database}}
    end

    def self.copy_table(source_db, dest_db, table_name)
      reload!
      src = config(source_db, :source)
      dst = config(dest_db, :destination)
      %x{ssh #{src.server_username}@#{src.server} 'mysqldump --skip-lock-tables --single-transaction -h#{src.host} -u#{src.username} -p"#{src.password}" #{src.database} #{table_name} | gzip -8' | gzip -cd | mysql -u#{dst.username} -p#{dst.password} #{dst.database}}
    end

    def self.incremental_copy(source_db, dest_db)
      reload!
      src = config(source_db, :source)
      dst = config(dest_db, :destination)
      Destination.establish_connection(dst)
      dst_ids = max_ids(Destination)
      src_ids = {}
      with_ssh_tunnel(src['server_username'], src['server'], 3307, src['host'], 3306) do
        conf = src.dup.tap do |db|
          db['host'] = '127.0.0.1'
          db['port'] = 3307
        end
        Source.establish_connection(conf)
        src_ids = max_ids(Source)
      end
      dst_ids.map do |table_name, dst_id|
        src_id   = src_ids[table_name]
        cmd_base = %{ssh #{src.server_username}@#{src.server} 'mysqldump --skip-lock-tables --single-transaction -h#{src.host} -u#{src.username} -p\\#{src.password.split('').join('\\')}}
        cmd_incr = %{--skip-add-drop-table --no-create-info --insert-ignore}
        cmd_id   = %{--where "id > #{dst_id}"}
        cmd_src  = %{#{src.database}}
        cmd_dst  = %{| gzip -9' | gzip -cd | mysql -u#{dst.username} -p#{dst.password} #{dst.database}}
        if dst_id && src_id && src_id > dst_id
          [ cmd_base, cmd_incr, cmd_id, cmd_src, table_name, cmd_dst ].join(' ').strip
        end
      end.compact
    end

    def self.sync(source_db, dest_db, time, small_threshold)
      reload!
      src              = config(source_db, :source)
      dst              = config(dest_db, :destination)
      db_info          = remote_table_info(source_db)
      incr_tables      = db_info.select { |k, v| v[:columns].map(&:to_sym).include?(:created_at) && v[:count].to_i > small_threshold }.keys
      whole_tables     = db_info.select { |k, v| !incr_tables.include?(k.to_sym) }.keys
      cmd_base         = %{ssh #{src.server_username}@#{src.server} 'mysqldump --skip-lock-tables --single-transaction -h#{src.host} -u#{src.username} -p\\#{src.password.split('').join('\\')}}
      cmd_time         = %{--where "created_at > \\"#{time}\\""}
      cmd_incr         = %{--skip-add-drop-table --no-create-info --insert-ignore}
      cmd_src          = %{#{src.database}}
      cmd_incr_tables  = %{#{incr_tables.map(&:to_s).join(' ')}}
      cmd_whole_tables = %{#{whole_tables.map(&:to_s).join(' ')}}
      cmd_dst          = %{| gzip -9' | gzip -cd | mysql -u#{dst.username} -p#{dst.password} #{dst.database}}
      incr_cmd         = [ cmd_base, cmd_time, cmd_incr, cmd_src, cmd_incr_tables, cmd_dst ].join(' ').strip
      whole_cmd        = [ cmd_base, cmd_src, cmd_whole_tables, cmd_dst ].join(' ').strip
      [ incr_cmd, whole_cmd ]
    end

    def self.config(name, purpose)
      checks  = %w(username password database)
      checks += %w(server host) if purpose == :source
      raise "configuration #{name} does not exist" unless @configs[name].present?
      errors = checks.select { |i| @configs[name][i].blank? }
      raise "#{errors.join(', ')} must be present in configuration #{name}" unless errors.blank?
      @configs[name]
    end

    def self.sources
      @configs.keys.select { |i| @configs[i].server.present? && @configs[i].server_username.present? }
    end

    def self.destinations
      @configs.keys - sources
    end

    def self.with_ssh_tunnel(gateway_user, gateway_host, local_port, remote_host, remote_port, &block)
      gateway = Net::SSH::Gateway.new(gateway_host, gateway_user)
      begin
        gateway.open(remote_host, remote_port, local_port)
        yield
      ensure
        gateway.shutdown!
      end
    end

    def self.remote_table_info(env_name)
      env_name = env_name.to_sym
      db_conf  = config(env_name.to_sym, :source).dup
      @table_info ||= {}
      @table_info[env_name] ||= {}
      begin
        with_ssh_tunnel(db_conf['server_username'], db_conf['server'], 3307, db_conf['host'], 3306) do
          db_conf['host'] = '127.0.0.1'
          db_conf['port'] = 3307
          ActiveRecord::Base.establish_connection(db_conf)
          conn   = ActiveRecord::Base.connection
          conn.tables.each do |t|
            @table_info[env_name][t.to_sym] = {
              count: (conn.execute("SELECT MAX(id) FROM #{t}").to_a.flatten[0] rescue nil),
              columns: conn.execute("SHOW COLUMNS FROM #{t}").to_a.map { |c| c[0] },
            }
          end
        end
      ensure
        ActiveRecord::Base.establish_connection(config(Rails.env, :destination))
      end
      @table_info[env_name]
    end

    def self.max_ids(ar_class)
      tables = ar_class.connection.tables - %w(schema_migrations)
      sql = "SELECT * FROM (#{tables.map { |t| "SELECT \"#{t}\", MAX(id) FROM `#{t}`" }.join(' UNION ')}) AS t"
      ar_class.connection.execute(sql).to_h
    end

    def self.define_methods!
      reload!
      sources.each do |src|
        destinations.each do |dst|
          define_singleton_method("copy_from_#{src}_to_#{dst}") do
            copy(src, dst)
          end
        end
      end
    end

  end
end
