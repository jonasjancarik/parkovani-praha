import React from "react";
import { createRoot } from "react-dom/client";
import { AddressPage } from "./AddressPage.jsx";
import { App } from "./App.jsx";
import { OverviewPage } from "./OverviewPage.jsx";
import { pageFromSearch } from "./overviewState.js";
import "./styles.css";

const PAGES = { address: AddressPage, explorer: App, overview: OverviewPage };
const Page = PAGES[pageFromSearch(window.location.search)] ?? OverviewPage;

createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <Page />
  </React.StrictMode>,
);
