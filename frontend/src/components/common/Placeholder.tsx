import styles from "./Placeholder.module.css";

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
    <div className={styles.root}>
      <span className={styles.kicker}>{kicker}</span>
      <h1 className={styles.title}>{title}</h1>
      <p className={styles.body}>{body}</p>
      <div className={styles.badge}>
        <span className={styles.badgeDot} />
        in design · rebuild in progress
      </div>
    </div>
  );
}
