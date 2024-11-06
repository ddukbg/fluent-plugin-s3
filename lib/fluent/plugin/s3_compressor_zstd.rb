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
        # 기존 gzip 압축기와 유사한 방식으로 구현
        w = StringIO.new
        chunk.write_to(w)
        w.rewind
        
        # zstd 압축 수행
        compressed = Zstd.compress(w.read, level: @compress_config.level)
        
        # 압축된 데이터를 임시 파일에 쓰기
        tmp.binmode
        tmp.write(compressed)
        tmp.close
      rescue => e
        log.warn "zstd compression failed: #{e.message}"
        raise
      end
    end
  end
end