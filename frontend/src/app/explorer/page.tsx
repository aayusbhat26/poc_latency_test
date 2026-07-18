"use client";

import { useState, useEffect } from "react";
import axios from "axios";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Download, ChevronLeft, ChevronRight, Zap } from "lucide-react";

export default function ExplorerPage() {
  const [layer, setLayer] = useState("silver");
  const [table, setTable] = useState("default");
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState(1);
  const [latency, setLatency] = useState<number | null>(null);

  const fetchTableData = async (l: string, t: string, p: number) => {
    setLoading(true);
    try {
      const res = await axios.get(`http://localhost:8000/api/data/${l}/${t}?page=${p}&limit=50`);
      setData(res.data.data);
      setLatency(res.data.latency_ms);
    } catch (e) {
      console.error(e);
      setData([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchTableData(layer, table, page);
  }, [layer, table, page]);

  const handleDownloadCSV = () => {
    if (!data.length) return;
    const headers = Object.keys(data[0]).join(",");
    const rows = data.map(r => Object.values(r).map(v => `"${v}"`).join(",")).join("\n");
    const csv = `${headers}\n${rows}`;
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${layer}_${table}_export.csv`;
    a.click();
  };

  const handleDownloadJSON = () => {
    if (!data.length) return;
    const json = JSON.stringify(data, null, 2);
    const blob = new Blob([json], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${layer}_${table}_export.json`;
    a.click();
  };

  return (
    <div className="flex flex-col space-y-6">
      <div className="flex justify-between items-end">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Data Explorer</h1>
          <p className="text-muted-foreground mt-2">Paginated view of Delta Tables via DuckDB</p>
        </div>
        <div className="flex gap-2">
          {latency !== null && (
            <Badge variant="secondary" className="flex items-center gap-1 px-3 py-1 text-sm bg-emerald-500/15 text-emerald-500 border-emerald-500/20">
              <Zap className="h-3 w-3" />
              Loaded in {latency} ms
            </Badge>
          )}
        </div>
      </div>

      <Card className="border-muted bg-card/40 backdrop-blur-md">
        <CardHeader className="pb-3 border-b border-border/50">
          <div className="flex justify-between items-center">
            <div className="flex gap-4">
              <Select value={layer} onValueChange={(v) => { setLayer(v); setTable(v === "gold" ? "fact_hourly_telemetry" : "default"); setPage(1); }}>
                <SelectTrigger className="w-[180px]">
                  <SelectValue placeholder="Select Layer" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="batch">Raw (Batch)</SelectItem>
                  <SelectItem value="staging">Staging</SelectItem>
                  <SelectItem value="silver">Silver</SelectItem>
                  <SelectItem value="gold">Gold</SelectItem>
                </SelectContent>
              </Select>

              {layer === "gold" && (
                <Select value={table} onValueChange={(v) => { setTable(v); setPage(1); }}>
                  <SelectTrigger className="w-[220px]">
                    <SelectValue placeholder="Select Table" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="dim_device">dim_device</SelectItem>
                    <SelectItem value="dim_time">dim_time</SelectItem>
                    <SelectItem value="dim_fault_codes">dim_fault_codes</SelectItem>
                    <SelectItem value="dim_status_flags">dim_status_flags</SelectItem>
                    <SelectItem value="fact_hourly_telemetry">fact_hourly_telemetry</SelectItem>
                    <SelectItem value="fact_fault_events">fact_fault_events</SelectItem>
                    <SelectItem value="mart_daily_costs">mart_daily_costs</SelectItem>
                    <SelectItem value="mart_shift_performance">mart_shift_performance</SelectItem>
                    <SelectItem value="mart_sla_breach_summary">mart_sla_breach_summary</SelectItem>
                    <SelectItem value="mart_compressor_health">mart_compressor_health</SelectItem>
                    <SelectItem value="mart_environmental_compliance">mart_environmental_compliance</SelectItem>
                    <SelectItem value="mart_pump_maintenance">mart_pump_maintenance</SelectItem>
                    <SelectItem value="mart_cooling_efficiency">mart_cooling_efficiency</SelectItem>
                    <SelectItem value="mart_valve_operations">mart_valve_operations</SelectItem>
                    <SelectItem value="mart_filter_diagnostics">mart_filter_diagnostics</SelectItem>
                  </SelectContent>
                </Select>
              )}
            </div>
            <div className="flex gap-2">
              <Button variant="outline" size="sm" onClick={handleDownloadCSV} disabled={data.length === 0}>
                <Download className="mr-2 h-4 w-4" /> CSV
              </Button>
              <Button variant="outline" size="sm" onClick={handleDownloadJSON} disabled={data.length === 0}>
                <Download className="mr-2 h-4 w-4" /> JSON
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent className="p-0">
          <div className="overflow-x-auto">
            <Table>
              <TableHeader className="bg-muted/50">
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
                {loading ? (
                  <TableRow>
                    <TableCell colSpan={100} className="text-center py-10 text-muted-foreground">
                      Executing DuckDB Query...
                    </TableCell>
                  </TableRow>
                ) : data.length > 0 ? (
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
                      No records found
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
          
          <div className="flex items-center justify-between p-4 border-t border-border/50">
            <div className="text-sm text-muted-foreground">
              Showing page {page} (Limit 50)
            </div>
            <div className="flex gap-2">
              <Button variant="outline" size="sm" onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1 || loading}>
                <ChevronLeft className="h-4 w-4 mr-1" /> Prev
              </Button>
              <Button variant="outline" size="sm" onClick={() => setPage(p => p + 1)} disabled={data.length < 50 || loading}>
                Next <ChevronRight className="h-4 w-4 ml-1" />
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
