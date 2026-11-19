// Swagger UI setup for kda-indexer. A separate file because the page's CSP allows no inline
// script. Everything it loads comes from this origin.
window.addEventListener("load", function () {
  var native = "Native API (/api/v1)";
  var compat = "chainweb-data compatible API (/txs, /stats, /coins, /v1/txs)";
  var isCompat = document.body.getAttribute("data-primary") === "compat";
  // Both documents are offered, but "Try it out" calls this origin, which serves only its
  // own routes: the other listener's document is labelled so.
  var other = " - other listener: Try it out does not work here";
  var own = { url: isCompat ? "/docs/openapi-compat.json" : "/docs/openapi.json",
              name: isCompat ? compat : native };
  var foreign = { url: isCompat ? "/docs/openapi.json" : "/docs/openapi-compat.json",
                  name: (isCompat ? native : compat) + other };
  window.ui = SwaggerUIBundle({
    urls: [own, foreign],
    "urls.primaryName": own.name,
    dom_id: "#swagger-ui",
    deepLinking: true,
    validatorUrl: null,
    supportedSubmitMethods: ["get"],
    presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
    layout: "StandaloneLayout"
  });
});
