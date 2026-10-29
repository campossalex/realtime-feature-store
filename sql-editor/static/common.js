// Shared by the SQL editor (index.html) and the pipeline page (pipeline.html). Loaded
// after CodeMirror and its sql mode, before each page's own script. Both pages are on the
// same origin and use the same storage keys, so they share one gateway session.

const $ = id => document.getElementById(id);

// The session and the editor text live in localStorage, so a reload, a closed tab or a
// second tab gets back the same session — and with it every table the attendee created.
// Wrapped because storage can be unavailable (private windows, blocked site data).
const store = {
  get: k => { try { return localStorage.getItem("sqlEditor." + k); } catch { return null; } },
  set: (k, v) => { try { v === null ? localStorage.removeItem("sqlEditor." + k) : localStorage.setItem("sqlEditor." + k, v); } catch {} },
};

// text/x-sql knows only a few dozen ANSI keywords, so most of Flink's DDL (IF NOT EXISTS,
// WATERMARK FOR, INTERVAL, CATALOG, ...) rendered as plain text. Same mode, more words.
(() => {
  const base = CodeMirror.resolveMode("text/x-sql");
  const words = s => Object.fromEntries(s.split(" ").map(w => [w, true]));
  CodeMirror.defineMIME("text/x-flinksql", { ...base,
    keywords: { ...base.keywords, ...words(
      "if exists with watermark for interval second seconds minute minutes hour hours day days " +
      "month year to catalog catalogs database databases show tables views functions use reset " +
      "explain describe primary key enforced constraint partitioned partition over range rows " +
      "preceding following current row unbounded lateral unnest system_time of tumble hop " +
      "cumulate session descriptor proctime filter cast coalesce case when then else end " +
      "returns temporary view function load module modules execute statement begin " +
      "full outer left right inner cross natural using except intersect all any some " +
      "max min sum avg first_value last_value row_number rank dense_rank lag lead") },
    builtin: { ...base.builtin, ...words(
      "string bytes varchar char boolean tinyint smallint int integer bigint float double " +
      "decimal numeric date time timestamp timestamp_ltz array map multiset raw") },
  });
})();

// Light/dark toggle (the #theme button). Until it is first clicked the page follows the
// OS setting (live, if the OS switches); a click pins the opposite of what is showing and
// remembers it. Wired once the page has loaded: this file runs in <head>.
const darkQuery = matchMedia("(prefers-color-scheme: dark)");
const isDark = () => (document.documentElement.dataset.theme || (darkQuery.matches ? "dark" : "light")) === "dark";
function paintThemeButton() {
  const b = $("theme"); if (!b) return;
  const next = isDark() ? "light" : "dark";
  b.textContent = isDark() ? "☀" : "☾";
  b.title = `Switch to ${next} mode`;
  b.setAttribute("aria-label", `Switch to ${next} mode`);
}
document.addEventListener("DOMContentLoaded", () => {
  $("theme").onclick = () => {
    const t = isDark() ? "light" : "dark";
    document.documentElement.dataset.theme = t;
    store.set("theme", t);
    paintThemeButton();
  };
  darkQuery.addEventListener("change", paintThemeButton);
  paintThemeButton();
});

async function api(path, body) {
  const r = await fetch(path, {
    method: body === undefined ? "GET" : "POST",
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const data = r.status === 204 ? {} : await r.json();
  if (!r.ok) throw new Error(data.error || r.statusText);
  return data;
}

async function copyText(t, btn) {
  try { await navigator.clipboard.writeText(t); }
  catch {   // no clipboard API outside a secure context (e.g. a remote host over http)
    const ta = Object.assign(document.createElement("textarea"), { value: t });
    document.body.append(ta); ta.select(); document.execCommand("copy"); ta.remove();
  }
  const was = btn.textContent; btn.textContent = "Copied"; setTimeout(() => btn.textContent = was, 1200);
}
