import Link from "next/link";
import { LayoutDashboard, Database, TerminalSquare } from "lucide-react";

export function Sidebar() {
  return (
    <div className="flex h-full w-64 flex-col border-r bg-card/50 backdrop-blur-xl p-4">
      <div className="mb-8 px-4">
        <h2 className="text-2xl font-bold tracking-tight bg-gradient-to-r from-blue-400 to-emerald-400 bg-clip-text text-transparent">
          Lakehouse UI
        </h2>
        <p className="text-xs text-muted-foreground mt-1">Medallion Architecture POC</p>
      </div>
      <nav className="flex-1 space-y-2">
        <Link href="/" className="flex items-center gap-3 rounded-lg px-4 py-2.5 text-sm font-medium text-muted-foreground hover:bg-accent hover:text-accent-foreground transition-all duration-200">
          <LayoutDashboard className="h-4 w-4" />
          Dashboard
        </Link>
        <Link href="/explorer" className="flex items-center gap-3 rounded-lg px-4 py-2.5 text-sm font-medium text-muted-foreground hover:bg-accent hover:text-accent-foreground transition-all duration-200">
          <Database className="h-4 w-4" />
          Data Explorer
        </Link>
        <Link href="/query" className="flex items-center gap-3 rounded-lg px-4 py-2.5 text-sm font-medium text-muted-foreground hover:bg-accent hover:text-accent-foreground transition-all duration-200">
          <TerminalSquare className="h-4 w-4" />
          SQL Workspace
        </Link>
      </nav>
      <div className="mt-auto px-4 text-xs text-muted-foreground">
        Powered by DuckDB + Delta
      </div>
    </div>
  );
}
