import { ThemeProvider } from "@/components/apx/theme-provider";
import { ModeToggle } from "@/components/apx/mode-toggle";
import { QueryClient } from "@tanstack/react-query";
import { createRootRouteWithContext, Link, Outlet } from "@tanstack/react-router";
import { Toaster } from "sonner";
import { Rocket, Database, Gauge, BookOpen, Lightbulb, DollarSign } from "lucide-react";
import { cn } from "@/lib/utils";

const NAV = [
  { to: "/", label: "Quickstart", icon: Rocket },
  { to: "/deployment", label: "Deployment", icon: Database },
  { to: "/testing", label: "Concurrency Testing", icon: Gauge },
  { to: "/cost", label: "Cost", icon: DollarSign },
  { to: "/best-practices", label: "Best Practices", icon: Lightbulb },
  { to: "/docs", label: "Docs", icon: BookOpen },
] as const;

function AppShell() {
  return (
    <div className="flex h-screen w-screen overflow-hidden bg-background text-foreground">
      <aside className="flex w-60 shrink-0 flex-col border-r bg-card/40">
        <div className="flex h-16 items-center gap-2 border-b px-5">
          <Database className="h-5 w-5 text-primary" />
          <span className="font-semibold tracking-tight">Lakebase Accelerator</span>
        </div>
        <nav className="flex-1 space-y-1 p-3">
          {NAV.map(({ to, label, icon: Icon }) => (
            <Link
              key={to}
              to={to}
              className={cn(
                "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground",
              )}
              activeProps={{ className: "bg-accent text-foreground" }}
              activeOptions={{ exact: to === "/" }}
            >
              <Icon className="h-4 w-4" />
              {label}
            </Link>
          ))}
        </nav>
        <div className="border-t p-3">
          <ModeToggle />
        </div>
      </aside>

      <main className="flex-1 overflow-y-auto">
        <Outlet />
      </main>
      <Toaster richColors />
    </div>
  );
}

export const Route = createRootRouteWithContext<{
  queryClient: QueryClient;
}>()({
  component: () => (
    <ThemeProvider defaultTheme="light" storageKey="apx-ui-theme">
      <AppShell />
    </ThemeProvider>
  ),
});
