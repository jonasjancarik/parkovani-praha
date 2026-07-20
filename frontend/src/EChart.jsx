import { useEffect, useRef } from "react";
import * as echarts from "echarts";

export function EChart({ option, className = "" }) {
  const elementRef = useRef(null);

  useEffect(() => {
    if (!elementRef.current) return undefined;
    const chart = echarts.init(elementRef.current, null, { renderer: "canvas" });
    chart.setOption(option, { notMerge: true });
    const observer = new ResizeObserver(() => chart.resize());
    observer.observe(elementRef.current);
    return () => {
      observer.disconnect();
      chart.dispose();
    };
  }, [option]);

  return <div ref={elementRef} className={className} />;
}
