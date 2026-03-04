export default {
  async fetch(request, env) {
    // CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type",
        },
      });
    }

    if (request.method !== "POST") {
      return new Response("POST only", { status: 405 });
    }

    // Trigger GitHub Actions via repository_dispatch
    const resp = await fetch(
      "https://api.github.com/repos/parthreddy123/oil-dashboard/dispatches",
      {
        method: "POST",
        headers: {
          Accept: "application/vnd.github+json",
          Authorization: `Bearer ${env.GITHUB_TOKEN}`,
          "Content-Type": "application/json",
          "User-Agent": "oil-dashboard-refresh-worker",
        },
        body: JSON.stringify({ event_type: "refresh" }),
      }
    );

    return new Response(
      JSON.stringify({ status: resp.status === 204 ? "triggered" : "error", code: resp.status }),
      {
        status: resp.status === 204 ? 200 : 502,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
      }
    );
  },
};
