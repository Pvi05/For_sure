require "open3"
require "json"

class CheckAnalysisJob < ApplicationJob
  queue_as :default

  def perform(job_id, url, root)
    env = load_dotenv(File.join(root, "pipeline/.env"))
              .merge("PYTHONPATH" => root)

    logs = []

    Open3.popen2e(env, "python3", File.join(root, "pipeline/main.py"), url, chdir: root) do |_in, out_err, wait|
      out_err.each_line do |line|
        line = line.chomp
        logs << line
        Rails.cache.write("job:#{job_id}", { status: "running", logs: logs }, expires_in: 30.minutes)
      end

      if wait.value.success?
        graph     = extract_json(logs.join("\n"), root)
        synthesis = extract_synthesis(logs.join("\n"), root)
        Rails.cache.write("job:#{job_id}", {
          status: "done", logs: logs, graph: graph, synthesis: synthesis
        }, expires_in: 30.minutes)
      else
        Rails.cache.write("job:#{job_id}", {
          status: "error", logs: logs
        }, expires_in: 10.minutes)
      end
    end
  end

  private

  def extract_json(output, root)
    dir = parse_output_dir(output, root)
    return nil unless dir
    JSON.parse(File.read(File.join(dir, "provenance.json"))) rescue nil
  end

  def extract_synthesis(output, root)
    dir = parse_output_dir(output, root)
    return nil unless dir
    JSON.parse(File.read(File.join(dir, "synthesis.json"))) rescue nil
  end

  def parse_output_dir(output, root)
    line = output.lines.find { |l| l.include?("Fichiers dans") }
    return nil unless line
    path = line.split("Fichiers dans").last.to_s.sub(/\A\s*:\s*/, "").strip.chomp("/")
    File.exist?(path) ? path : nil
  end

  def load_dotenv(path)
    return {} unless File.exist?(path)
    File.readlines(path, chomp: true).each_with_object({}) do |line, env|
      line = line.sub(/\A\s*export\s+/, "").strip
      next if line.empty? || line.start_with?("#")
      key, val = line.split("=", 2)
      next unless key && val
      env[key.strip] = val.strip.gsub(/\A['"]|['"]\z/, "")
    end
  end
end