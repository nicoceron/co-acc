import { useEffect, useRef, useState } from "react";

export interface AsyncState<T> {
  data: T | null;
  error: Error | null;
  loading: boolean;
}

export function useAsync<T>(fn: () => Promise<T>, deps: ReadonlyArray<unknown> = []): AsyncState<T> {
  const [state, setState] = useState<AsyncState<T>>({
    data: null,
    error: null,
    loading: true,
  });

  const fnRef = useRef(fn);
  fnRef.current = fn;

  useEffect(() => {
    let cancelled = false;
    setState((prev) => ({ ...prev, loading: true }));
    fnRef
      .current()
      .then((data) => {
        if (cancelled) return;
        setState({ data, error: null, loading: false });
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        const error = err instanceof Error ? err : new Error(String(err));
        setState({ data: null, error, loading: false });
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  return state;
}
