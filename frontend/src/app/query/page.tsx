"use client";

import { useState } from "react";
import axios from "axios";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Play, Zap, TerminalSquare } from "lucide-react";

export default function QueryPage() {
  const [query, setQuery] = useState("SELECT * FROM silver LIMIT 10");
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [latency, setLatency] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);

  const runQuery = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await axios.post(`http://localhost:8000/api/query`, { query });
      setData(res.data.data);
      setLatency(res.data.latency_ms);
    } catch (e: any) {
      console.error(e);
      setError(e.response?.data?.detail || e.message || "Failed to execute query");
      setData([]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex flex-col space-y-6">
      <div className="flex justify-between items-end">
        <div>
          <h1 className="text-3xl font-bold tracking-tight flex items-center gap-2">
            <TerminalSquare className="h-8 w-8 text-emerald-400" />
            SQL Workspace
          </h1>
          <p className="text-muted-foreground mt-2">Write raw SQL queries against Delta Tables via DuckDB</p>
        </div>
      </div>

      <Card className="border-muted bg-card/40 backdrop-blur-md">
        <CardHeader className="pb-3 border-b border-border/50">
          <CardTitle className="text-lg">Query Editor</CardTitle>
        </CardHeader>
        <CardContent className="p-4">
          <textarea
            className="w-full h-32 p-4 bg-black/50 border border-border/50 rounded-lg font-mono text-sm focus:outline-none focus:ring-1 focus:ring-emerald-500 text-emerald-300"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            spellCheck={false}
          />
          <div className="mt-4 flex justify-between items-center">
            <div className="text-xs text-muted-foreground">
              Available tables: <code className="text-emerald-400">batch, staging, silver, gold_fact_hourly_telemetry, gold_mart_daily_costs...</code>
            </div>
            <Button onClick={runQuery} disabled={loading} className="bg-emerald-600 hover:bg-emerald-700 text-white">
              <Play className="mr-2 h-4 w-4" /> {loading ? "Executing..." : "Run Query"}
            </Button>
          </div>
        </CardContent>
      </Card>

      <Card className="border-muted bg-card/40 backdrop-blur-md flex-1 flex flex-col">
        <CardHeader className="pb-3 border-b border-border/50 flex flex-row justify-between items-center">
          <CardTitle className="text-lg">Results</CardTitle>
          {latency !== null && !error && (
            <Badge variant="secondary" className="flex items-center gap-1 px-3 py-1 text-sm bg-emerald-500/15 text-emerald-500 border-emerald-500/20">
              <Zap className="h-3 w-3" />
              Executed in {latency} ms
            </Badge>
          )}
        </CardHeader>
        <CardContent className="p-0 flex-1 overflow-auto">
          {error ? (
            <div className="p-8 text-center text-red-400 bg-red-950/20 m-4 rounded-lg border border-red-900/50">
              <div className="font-bold mb-2">Query Error</div>
              <div className="font-mono text-xs">{error}</div>
            </div>
          ) : (
            <div className="overflow-x-auto h-[400px]">
              <Table>
                <TableHeader className="bg-muted/50 sticky top-0 z-10">
                  <TableRow>
                    {data.length > 0 ? (
                      Object.keys(data[0]).map((k) => (
                        <TableHead key={k} className="whitespace-nowrap">{k}</TableHead>
                      ))
                    ) : (
                      <TableHead>No Data</TableHead>
                    )}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.length > 0 ? (
                    data.map((row, i) => (
                      <TableRow key={i}>
                        {Object.values(row).map((val: any, j) => (
                          <TableCell key={j} className="whitespace-nowrap">
                            {val !== null ? String(val) : <span className="text-muted-foreground opacity-50">null</span>}
                          </TableCell>
                        ))}
                      </TableRow>
                    ))
                  ) : (
                    <TableRow>
                      <TableCell colSpan={100} className="text-center py-10 text-muted-foreground">
                        Run a query to see results
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
