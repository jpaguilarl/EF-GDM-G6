import { NavLink, Outlet } from "react-router-dom";
import { Settings, Radio, Play, BarChart3, ClipboardList } from "lucide-react";

const navItems = [
  { to: "/configuration", label: "Configuración", icon: Settings },
  { to: "/realtime", label: "Tiempo Real", icon: Radio },
  { to: "/batch", label: "Carga por Lotes", icon: Play },
  { to: "/gold", label: "Resultados Gold", icon: BarChart3 },
  { to: "/audit", label: "Auditoría", icon: ClipboardList },
];

export function Layout() {
  return (
    <div className="flex min-h-screen bg-background">
      {/* Left Nav */}
      <nav className="w-64 bg-surface-container-lowest border-r border-border-subtle flex flex-col">
        <div className="p-6 border-b border-border-subtle">
          <h1 className="text-2xl text-primary-container font-bold tracking-tight">
            Panel de Control
          </h1>
          <p className="text-xs text-on-surface-variant mt-1">
            TLC Pipeline
          </p>
        </div>
        <div className="flex-1 p-4 space-y-1">
          {navItems.map(({ to, label, icon: Icon }) => (
            <NavLink
              key={to}
              to={to}
              className={({ isActive }) =>
                `flex items-center gap-3 px-4 py-3 rounded text-sm transition-colors ${
                  isActive
                    ? "bg-primary-container/10 text-primary-container font-semibold"
                    : "text-on-surface-variant hover:bg-surface-muted"
                }`
              }
            >
              <Icon className="w-5 h-5" />
              {label}
            </NavLink>
          ))}
        </div>
      </nav>

      {/* Content */}
      <main className="flex-1 p-8 max-w-container mx-auto w-full">
        <Outlet />
      </main>
    </div>
  );
}
