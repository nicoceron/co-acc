export function Placeholder({
  kicker,
  title,
  body,
}: {
  kicker: string;
  title: string;
  body: string;
}) {
  return (
    <div className="mx-auto flex max-w-[780px] flex-col gap-4 px-8 py-16">
      <span className="font-mono text-[11px] uppercase tracking-[0.14em] text-lime-300">
        {kicker}
      </span>
      <h1 className="text-3xl font-medium tracking-tight text-ink-50 md:text-4xl">{title}</h1>
      <p className="max-w-[640px] text-[15px] leading-relaxed text-ink-400">{body}</p>
      <div className="mt-2 inline-flex items-center gap-2 self-start rounded-md border border-dashed border-white/15 px-3 py-2 font-mono text-[11px] uppercase tracking-wider text-ink-500">
        <span className="h-1.5 w-1.5 rounded-full bg-amber-300 shadow-[0_0_6px_var(--color-amber-300)]" />
        in design · rebuild in progress
      </div>
    </div>
  );
}
