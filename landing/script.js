(() => {
  "use strict";

  const header = document.querySelector("[data-site-header]");
  if (header) {
    const updateHeader = () => {
      header.classList.toggle("is-scrolled", window.scrollY > 12);
    };

    updateHeader();
    window.addEventListener("scroll", updateHeader, { passive: true });
  }

  const year = document.querySelector("[data-current-year]");
  if (year) {
    year.textContent = String(new Date().getFullYear());
  }

  const fallbackCopy = (text) => {
    const helper = document.createElement("textarea");
    helper.className = "clipboard-helper";
    helper.value = text;
    helper.setAttribute("readonly", "");
    document.body.appendChild(helper);
    helper.select();

    let copied = false;
    try {
      copied = document.execCommand("copy");
    } catch {
      copied = false;
    }

    helper.remove();
    return copied;
  };

  const copyText = async (text) => {
    if (navigator.clipboard && window.isSecureContext) {
      try {
        await navigator.clipboard.writeText(text);
        return true;
      } catch {
        return fallbackCopy(text);
      }
    }
    return fallbackCopy(text);
  };

  document.querySelectorAll("[data-copy-target]").forEach((button) => {
    const targetId = button.getAttribute("data-copy-target");
    const target = targetId ? document.getElementById(targetId) : null;
    const label = button.querySelector("span");
    const status = document.querySelector("[data-copy-status]");

    if (!target || !label) {
      return;
    }

    button.addEventListener("click", async () => {
      const copied = await copyText(target.textContent.trim());
      if (!copied) {
        if (status) {
          status.textContent = "Copy failed. Select the command manually.";
        }
        return;
      }

      const originalLabel = "Copy";
      label.textContent = "Copied";
      button.classList.add("is-copied");
      if (status) {
        status.textContent = "Quickstart commands copied to clipboard.";
      }

      window.setTimeout(() => {
        label.textContent = originalLabel;
        button.classList.remove("is-copied");
      }, 2000);
    });
  });
})();
