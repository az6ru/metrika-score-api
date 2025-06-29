create table if not exists public.tasks (
  id uuid primary key,
  status text not null,
  progress integer not null,
  message text,
  started_at timestamptz,
  finished_at timestamptz,
  error text,
  params jsonb
);

create table if not exists public.results (
  task_id uuid primary key references public.tasks(id) on delete cascade,
  result_json jsonb
);
