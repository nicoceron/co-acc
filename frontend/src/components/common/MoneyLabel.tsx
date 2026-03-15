import styles from "./MoneyLabel.module.css";

interface MoneyLabelProps {
  value: number;
  className?: string;
}

export function MoneyLabel({ value, className }: MoneyLabelProps) {
  const formatted = value.toLocaleString("es-CO", {
    style: "currency",
    currency: "COP",
    maximumFractionDigits: 0,
  });

  return <span className={`${styles.money} ${className ?? ""}`}>{formatted}</span>;
}
