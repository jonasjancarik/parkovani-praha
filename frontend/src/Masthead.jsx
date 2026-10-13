import { IconCalendar } from "@tabler/icons-react";

import { isoToCzech } from "./format.js";

const PAGES = [
  { id: "overview", label: "Městský přehled", href: "./" },
  { id: "explorer", label: "Datový průzkumník", href: "./?page=explorer" },
  { id: "address", label: "Okolí adresy", href: "./?page=address" },
];

export function Masthead({ page, dataDate }) {
  return (
    <header className="masthead">
      <a className="brand" href="./">Parkování Praha</a>
      <nav aria-label="Hlavní navigace">
        {PAGES.map((item) => (
          <a
            key={item.id}
            href={item.href}
            className={item.id === page ? "active" : ""}
            aria-current={item.id === page ? "page" : undefined}
          >
            {item.label}
          </a>
        ))}
      </nav>
      <span className="data-date">Data k {isoToCzech(dataDate)} <IconCalendar size={17} stroke={1.5} /></span>
    </header>
  );
}
