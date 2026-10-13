import React from "react";
import { createRoot } from "react-dom/client";
import { AddressPage } from "./AddressPage.jsx";
import { App } from "./App.jsx";
import "./styles.css";

const page = new URLSearchParams(window.location.search).get("page");

createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    {page === "address" ? <AddressPage /> : <App />}
  </React.StrictMode>,
);
