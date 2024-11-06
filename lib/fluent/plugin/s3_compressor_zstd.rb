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
        begin
          log.debug "Starting ZSTD compression"
          chunk.write_to(tmp) do |chunk_io|
            data = chunk_io.read
            log.debug "Read data size: #{data.bytesize}"
            
            compressed = Zstd.compress(data, level: @compress_config.level)
            log.debug "Compressed data size: #{compressed.bytesize}"
            log.debug "First few bytes of compressed data: #{compressed[0..10].bytes.map { |b| sprintf('%02x', b) }.join(' ')}"
            
            tmp.rewind
            tmp.binmode
            tmp.write(compressed)
            tmp.flush
          end
          log.debug "Finished ZSTD compression"
        rescue => e
          log.warn "ZSTD compression failed: #{e.message}"
          log.warn e.backtrace.join("\n")
          raise
        end
      end
    end
  end
end