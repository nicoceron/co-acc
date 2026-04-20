export function Spinner({ label }: { label?: string }) {
  return (
    <div className="inline-flex items-center gap-2 p-3 text-[13px] text-ink-400" role="status" aria-live="polite">
      <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-lime-400 [animation-delay:0ms]" />
      <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-lime-400 [animation-delay:150ms]" />
      <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-lime-400 [animation-delay:300ms]" />
      {label ? (
        <span className="ml-1 font-mono text-[11px] uppercase tracking-wider">{label}</span>
      ) : null}
    </div>
  );
}
