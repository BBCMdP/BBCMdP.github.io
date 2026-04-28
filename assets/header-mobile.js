(function () {
  function buildMobileHeaderMenu(header) {
    const nav = header.querySelector('nav');
    const toggleButton = header.querySelector('button.md\\:hidden');

    if (!nav || !toggleButton || toggleButton.dataset.mobileMenuReady === 'true') {
      return;
    }

    const headerRow = header.querySelector(':scope > div');
    const widthClass = headerRow
      ? Array.from(headerRow.classList).find(function (cls) {
          return cls.indexOf('max-w-') === 0;
        })
      : null;

    const mobileMenu = document.createElement('div');
    mobileMenu.className = 'mobile-header-menu hidden md:hidden w-full border-t border-[#f0f2f4] dark:border-[#222] bg-white dark:bg-[#111318]';

    const menuInner = document.createElement('div');
    menuInner.className = (widthClass || 'max-w-[1200px]') + ' mx-auto px-4 md:px-8 py-4 flex flex-col gap-4';

    const linksContainer = document.createElement('div');
    linksContainer.className = 'flex flex-col';

    nav.querySelectorAll('a[href]').forEach(function (link) {
      const mobileLink = link.cloneNode(true);
      mobileLink.className = 'block py-2 text-sm font-medium hover:text-primary transition-colors';
      linksContainer.appendChild(mobileLink);
    });

    menuInner.appendChild(linksContainer);

    const langButtons = header.querySelectorAll('.lang-btn');
    if (langButtons.length > 0) {
      const langContainer = document.createElement('div');
      langContainer.className = 'inline-flex items-center gap-2 bg-[#f0f2f4] dark:bg-[#2d3748] rounded-lg p-1 w-fit';

      langButtons.forEach(function (btn) {
        const mobileBtn = btn.cloneNode(true);
        mobileBtn.classList.remove('hidden', 'md:flex');
        langContainer.appendChild(mobileBtn);
      });

      menuInner.appendChild(langContainer);
    }

    mobileMenu.appendChild(menuInner);
    header.appendChild(mobileMenu);

    const icon = toggleButton.querySelector('.material-symbols-outlined');

    function setMenuState(isOpen) {
      mobileMenu.classList.toggle('hidden', !isOpen);
      toggleButton.setAttribute('aria-expanded', String(isOpen));
      if (icon) {
        icon.textContent = isOpen ? 'close' : 'menu';
      }
    }

    toggleButton.addEventListener('click', function () {
      const isOpen = mobileMenu.classList.contains('hidden');
      setMenuState(isOpen);
    });

    mobileMenu.addEventListener('click', function (event) {
      if (event.target.closest('a')) {
        setMenuState(false);
      }
    });

    document.addEventListener('keydown', function (event) {
      if (event.key === 'Escape') {
        setMenuState(false);
      }
    });

    window.addEventListener('resize', function () {
      if (window.innerWidth >= 768) {
        setMenuState(false);
      }
    });

    toggleButton.setAttribute('type', 'button');
    if (!toggleButton.hasAttribute('aria-label')) {
      toggleButton.setAttribute('aria-label', 'Open menu');
    }

    setMenuState(false);
    toggleButton.dataset.mobileMenuReady = 'true';
  }

  document.addEventListener('DOMContentLoaded', function () {
    document.querySelectorAll('header').forEach(buildMobileHeaderMenu);
  });
})();
