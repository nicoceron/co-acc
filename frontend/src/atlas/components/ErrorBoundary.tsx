import { Component, type ErrorInfo, type ReactNode } from "react";

interface ErrorBoundaryState {
  hasError: boolean;
}

export class ErrorBoundary extends Component<{ children: ReactNode }, ErrorBoundaryState> {
  state: ErrorBoundaryState = { hasError: false };

  static getDerivedStateFromError(): ErrorBoundaryState {
    return { hasError: true };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("Atlas render error", error, info);
  }

  render() {
    if (this.state.hasError) {
      return (
        <main className="co-container co-error">
          <h1>No se pudo renderizar el atlas</h1>
          <p>Recarga la pagina. Si el problema continua, revisa la consola del navegador.</p>
        </main>
      );
    }

    return this.props.children;
  }
}
