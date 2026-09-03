/*
 * Follow Airflow's light/dark mode.
 *
 * Plugin pages render in an iframe that Airflow sandboxes with
 * "allow-same-origin", and they are served by the same API server as the UI, so
 * this can read the parent document. Airflow tracks its color mode with
 * next-themes, which resolves it to a data-theme attribute on <html>. Copying
 * that onto our own <html> lets bluecore.css react to it.
 *
 * Without this the page falls back to the prefers-color-scheme media query,
 * which only knows the operating system preference and so disagrees with
 * Airflow whenever someone uses its toggle.
 */
(function () {
  "use strict";

  var parentRoot;
  try {
    parentRoot = window.parent.document.documentElement;
  } catch (error) {
    return;
  }

  if (!parentRoot || parentRoot === document.documentElement) {
    return;
  }

  function sync() {
    var theme = parentRoot.getAttribute("data-theme");
    if (theme) {
      document.documentElement.setAttribute("data-theme", theme);
    }
  }

  sync();
  new MutationObserver(sync).observe(parentRoot, {
    attributeFilter: ["data-theme"],
  });
})();
