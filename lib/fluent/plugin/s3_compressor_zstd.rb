require 'zstd-ruby' 

module Fluent::Plugin
  class S3Output
    class ZstdCompressor < Compressor
      S3Output.register_compressor("zstd", self)

      config_section :compress, param_name: :compress_config, init: true, multi: false do
        desc "Compression level for zstd (1-22)"
        config_param :level, :integer, default: 3
      end

      def ext
        'zst'.freeze
      end

      def content_type
        'application/x-zst'.freeze
      end

      def compress(chunk, tmp)
        chunk.write_to(tmp) do |chunk_io|
          compressor = Zstd::Compressor.new(level: @compress_config.level)
          compressed = compressor.compress(chunk_io.read)
          tmp.write(compressed)
        end
      rescue => e
        log.warn "zstd compression failed: #{e.message}"
        raise
      end
    end
  end
end