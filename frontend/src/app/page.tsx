"use client";

import { useEffect, useState } from "react";
import axios from "axios";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Zap, Activity, DollarSign, AlertTriangle } from "lucide-react";
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, AreaChart, Area } from "recharts";
import LiveCharts from "@/components/LiveCharts";

export default function DashboardPage() {
  const [metrics, setMetrics] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [latency, setLatency] = useState<number | null>(null);

  useEffect(() => {
    const fetchDashboard = async () => {
      try {
        const res = await axios.get("http://localhost:8000/api/dashboard");
        setMetrics(res.data.metrics);
        setLatency(res.data.latency_ms);
      } catch (e) {
        console.error(e);
      } finally {
        setLoading(false);
      }
    };
    fetchDashboard();
  }, []);

  if (loading) {
    return <div className="flex h-full items-center justify-center text-muted-foreground animate-pulse">Loading Gold Layer Dashboards...</div>;
  }

  if (!metrics) {
    return <div className="flex h-full items-center justify-center text-muted-foreground">Please run the PySpark pipeline first to populate the Gold Layer Data Lake.</div>;
  }

  // Calculate totals
  const totalCost = metrics.daily_costs.reduce((sum: number, r: any) => sum + (r.total_cost || 0), 0);
  const totalBreaches = metrics.sla_breaches.reduce((sum: number, r: any) => sum + (r.total_breaches || 0), 0);
  const totalPower = metrics.daily_costs.reduce((sum: number, r: any) => sum + (r.total_power || 0), 0);

  return (
    <div className="flex flex-col h-full space-y-8 overflow-y-auto pb-10 pr-2">
      <div className="flex justify-between items-end">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Executive Dashboard</h1>
          <p className="text-muted-foreground mt-2">Aggregated metrics served directly from Delta Gold layer via DuckDB</p>
        </div>
        {latency !== null && (
          <Badge variant="secondary" className="flex items-center gap-1 px-3 py-1 text-sm bg-emerald-500/15 text-emerald-500 border-emerald-500/20">
            <Zap className="h-3 w-3" />
            Loaded in {latency} ms
          </Badge>
        )}
      </div>

      <div className="mb-8 border-b border-border pb-8">
        <LiveCharts />
      </div>

      <div className="grid gap-6 md:grid-cols-3">
        <Card className="bg-gradient-to-br from-card to-card/50 border-emerald-900/50 backdrop-blur-md">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-emerald-400">Total Operating Cost</CardTitle>
            <DollarSign className="h-4 w-4 text-emerald-500" />
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold">${totalCost.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
            <p className="text-xs text-muted-foreground mt-1">Aggregated from `mart_daily_costs`</p>
          </CardContent>
        </Card>

        <Card className="bg-gradient-to-br from-card to-card/50 border-blue-900/50 backdrop-blur-md">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-blue-400">Total Power Consumption</CardTitle>
            <Activity className="h-4 w-4 text-blue-500" />
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold">{totalPower.toLocaleString(undefined, { maximumFractionDigits: 0 })} kW</div>
            <p className="text-xs text-muted-foreground mt-1">Aggregated from `mart_daily_costs`</p>
          </CardContent>
        </Card>

        <Card className="bg-gradient-to-br from-card to-card/50 border-rose-900/50 backdrop-blur-md">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-rose-400">SLA Breaches</CardTitle>
            <AlertTriangle className="h-4 w-4 text-rose-500" />
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold text-rose-300">{totalBreaches}</div>
            <p className="text-xs text-muted-foreground mt-1">Aggregated from `mart_sla_breach_summary`</p>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-6 md:grid-cols-2">
        <Card className="border-muted bg-card/40 backdrop-blur-md">
          <CardHeader>
            <CardTitle>Daily Cost Trend</CardTitle>
            <CardDescription>Visualizing spending over time</CardDescription>
          </CardHeader>
          <CardContent className="h-[300px]">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={metrics.daily_costs} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                <defs>
                  <linearGradient id="colorCost" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#10b981" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#10b981" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#333" vertical={false} />
                <XAxis dataKey="Date" stroke="#888" tickFormatter={(v) => String(v).split("-").slice(1).join("/")} />
                <YAxis stroke="#888" />
                <Tooltip contentStyle={{ backgroundColor: '#111', borderColor: '#333' }} />
                <Area type="monotone" dataKey="total_cost" stroke="#10b981" strokeWidth={2} fillOpacity={1} fill="url(#colorCost)" />
              </AreaChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card className="border-muted bg-card/40 backdrop-blur-md">
          <CardHeader>
            <CardTitle>Average Temperatures by Hour</CardTitle>
            <CardDescription>Entering vs Leaving Chilled Water</CardDescription>
          </CardHeader>
          <CardContent className="h-[300px]">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={metrics.hourly_temperatures} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#333" vertical={false} />
                <XAxis dataKey="Hour_of_Day" stroke="#888" tickFormatter={(v) => `${v}:00`} />
                <YAxis stroke="#888" domain={['auto', 'auto']} />
                <Tooltip contentStyle={{ backgroundColor: '#111', borderColor: '#333' }} />
                <Legend />
                <Line type="monotone" dataKey="entering" stroke="#ef4444" strokeWidth={2} dot={false} />
                <Line type="monotone" dataKey="leaving" stroke="#3b82f6" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card className="border-muted bg-card/40 backdrop-blur-md col-span-2">
          <CardHeader>
            <CardTitle>Power Consumption vs Cost</CardTitle>
            <CardDescription>Daily comparative view</CardDescription>
          </CardHeader>
          <CardContent className="h-[300px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={metrics.daily_costs} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#333" vertical={false} />
                <XAxis dataKey="Date" stroke="#888" />
                <YAxis yAxisId="left" stroke="#3b82f6" />
                <YAxis yAxisId="right" orientation="right" stroke="#10b981" />
                <Tooltip contentStyle={{ backgroundColor: '#111', borderColor: '#333' }} />
                <Legend />
                <Bar yAxisId="left" dataKey="total_power" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                <Bar yAxisId="right" dataKey="total_cost" fill="#10b981" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
