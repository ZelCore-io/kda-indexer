// Swagger UI setup for kda-indexer. A separate file because the page's CSP allows no inline
// script. Everything it loads comes from this origin.
window.addEventListener("load", function () {
  var native = "Native API (/api/v1)";
  var compat = "chainweb-data compatible API (/txs, /stats, /coins, /v1/txs)";
  var primary = document.body.getAttribute("data-primary") === "compat" ? compat : native;
  window.ui = SwaggerUIBundle({
    urls: [
      { url: "/docs/openapi.json", name: native },
      { url: "/docs/openapi-compat.json", name: compat }
    ],
    "urls.primaryName": primary,
    dom_id: "#swagger-ui",
    deepLinking: true,
    validatorUrl: null,
    supportedSubmitMethods: ["get"],
    presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
    layout: "StandaloneLayout"
  });
});
