require 'zstd-ruby'

module Fluent::Plugin
  class S3Output
    class ZstdCompressor < Compressor
      S3Output.register_compressor('zstd', self)

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
        w = StringIO.new
        chunk.write_to(w)
        w.rewind
        
        # zstd 압축 수행
        compressed = Zstd.compress(w.read, level: @compress_config.level)
        
        # 압축된 데이터를 임시 파일에 쓰기
        tmp.binmode
        tmp.rewind
        tmp.write(compressed)
        tmp.rewind  # 파일 포인터를 처음으로 되돌리기
      rescue => e
        log.warn "zstd compression failed: #{e.message}"
        raise
      end
    end
  end
end