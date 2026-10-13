import { useEffect, useState } from "react";

export function useMediaQuery(query) {
  const [matches, setMatches] = useState(() => window.matchMedia(query).matches);
  useEffect(() => {
    const media = window.matchMedia(query);
    const update = () => setMatches(media.matches);
    media.addEventListener("change", update);
    return () => media.removeEventListener("change", update);
  }, [query]);
  return matches;
}

export function useJson(url) {
  const [state, setState] = useState({ data: null, error: false, loading: Boolean(url) });
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    if (!url) {
      setState({ data: null, error: false, loading: false });
      return undefined;
    }
    const controller = new AbortController();
    setState((current) => ({ ...current, error: false, loading: true }));
    fetch(url, { signal: controller.signal })
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
      })
      .then((data) => setState({ data, error: false, loading: false }))
      .catch((reason) => {
        if (reason.name !== "AbortError") setState((current) => ({ ...current, error: true, loading: false }));
      });
    return () => controller.abort();
  }, [url, attempt]);
  return { ...state, retry: () => setAttempt((value) => value + 1) };
}
