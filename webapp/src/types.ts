export type Task = { task_id: string; state: string; try_number: number }
export type DagSnapshot = { dag_id: string; run: { dag_run_id: string; state: string; start_date?: string } | null; tasks: Task[] }
export type Config = { dagId: string; supabaseUrl: string; supabaseKey: string; broadcastTopic: string; voiceAvailable: boolean; voiceConfigError: string; liveModel: string; delegateModel: string }
export type Evidence = { id: string; name: string; args: Record<string, string>; result: unknown; at: string; preview?: boolean }
export type Activity = { id: string; text: string; kind: 'system' | 'agent' | 'tool' | 'error' | 'user'; at: string }
export type DashboardPoint = { date: string; revenueAud: number; completedTransactions: number; purchasingCustomers: number }
export type DashboardGroup = { name: string; revenueAud: number; purchasingCustomers: number }
export type DashboardSnapshot =
  | { available: false; message: string }
  | { available: true; period: string; latestDate: string; firstDate: string; customerTypeColumn: string | null;
      summary: { revenueAud: number; completedTransactions: number; purchasingCustomers: number; customerDays: number };
      daily: DashboardPoint[]; regions: DashboardGroup[]; customerTypes: DashboardGroup[] }
