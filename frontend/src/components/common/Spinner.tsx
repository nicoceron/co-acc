import styles from "./Spinner.module.css";

export function Spinner({ label }: { label?: string }) {
  return (
    <div className={styles.root} role="status" aria-live="polite">
      <div className={styles.dot} />
      <div className={styles.dot} />
      <div className={styles.dot} />
      {label ? <span className={styles.label}>{label}</span> : null}
    </div>
  );
}
