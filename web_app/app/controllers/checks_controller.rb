class ChecksController < ApplicationController
  PIPELINE_ROOT = File.expand_path("../../..", __dir__)  # /Users/paulv/sample

  def new
  end

  BLUESKY_URL_RE = %r{
    \A(
      https://bsky\.app/profile/[^/]+/post/\w+   # URL web Bluesky
      |
      at://did:[^/]+/app\.bsky\.feed\.post/\w+   # URI AT Protocol
    )\z
  }x

  def create
    @claim = params[:claim].to_s.strip
    return redirect_to root_path, alert: "URL manquante." if @claim.empty?

    unless @claim.match?(BLUESKY_URL_RE)
      return redirect_to root_path,
        alert: "Merci de coller l'URL d'un post Bluesky (ex: https://bsky.app/profile/…/post/…)"
    end

    job_id = SecureRandom.uuid
    Rails.cache.write("job:#{job_id}", { status: "pending" }, expires_in: 10.minutes)

    CheckAnalysisJob.perform_later(job_id, @claim, PIPELINE_ROOT)

    redirect_to check_path(job_id)
  end

  def show
    @job_id = params[:id]
    @payload = Rails.cache.read("job:#{@job_id}") || { status: "not_found" }
  end

  def status
    job_id  = params[:id]
    payload = Rails.cache.read("job:#{job_id}") || { status: "not_found" }
    render json: payload
  end
end