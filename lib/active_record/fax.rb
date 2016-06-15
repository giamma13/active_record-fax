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

    def self.reload!
      @configs = Hashie::Mash.new(YAML.load_file(File.join(Rails.application.root, 'config', 'database.yml')))
    end

    def self.copy(source_db, dest_db)
      reload!
      src = config(source_db, :source)
      dst = config(dest_db, :destination)
      %x{ssh #{src.server_username}@#{src.server} 'mysqldump --skip-lock-tables --single-transaction -h#{src.host} -u#{src.username} -p"#{src.password}" #{src.database} | gzip -9' | gzip -cd | mysql -u#{dst.username} -p#{dst.password} #{dst.database}}
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
