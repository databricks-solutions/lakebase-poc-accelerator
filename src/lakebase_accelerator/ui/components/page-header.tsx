interface PageHeaderProps {
  title: string;
  description?: string;
}

export function PageHeader({ title, description }: PageHeaderProps) {
  return (
    <div className="border-b px-8 py-6">
      <h1 className="text-2xl font-semibold tracking-tight">{title}</h1>
      {description && (
        <p className="mt-1 max-w-3xl text-sm text-muted-foreground">{description}</p>
      )}
    </div>
  );
}

export default PageHeader;
