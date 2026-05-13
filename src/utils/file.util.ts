export function tryDecodeBase64Text(base64: string): string | null {
  try {
    const decoded = Buffer.from(base64, 'base64').toString('utf-8');
    const reEncoded = Buffer.from(decoded, 'utf-8').toString('base64');
    if (reEncoded.replace(/=/g, '') !== base64.replace(/=/g, '')) {
      return null;
    }
    return decoded;
  } catch {
    return null;
  }
}

export function isTextFile(fileName: string): boolean {
  const textExtensions = [
    '.txt', '.md', '.json', '.yaml', '.yml', '.xml', '.csv',
    '.ts', '.js', '.jsx', '.tsx', '.py', '.rb', '.go', '.rs',
    '.java', '.c', '.cpp', '.h', '.hpp', '.cs', '.swift', '.kt',
    '.sh', '.bash', '.zsh', '.fish',
    '.html', '.css', '.scss', '.less', '.vue', '.svelte',
    '.sql', '.graphql', '.proto', '.toml', '.ini', '.cfg', '.conf',
    '.dockerfile', '.makefile', '.cmake',
    '.gitignore', '.env', '.editorconfig',
    '.log', '.diff', '.patch',
  ];
  const lower = fileName.toLowerCase();
  return textExtensions.some(ext => lower.endsWith(ext));
}

export function getMimeType(fileName: string): string {
  const mimeMap: Record<string, string> = {
    '.txt': 'text/plain',
    '.md': 'text/markdown',
    '.json': 'application/json',
    '.yaml': 'text/yaml',
    '.yml': 'text/yaml',
    '.xml': 'text/xml',
    '.csv': 'text/csv',
    '.html': 'text/html',
    '.css': 'text/css',
    '.js': 'application/javascript',
    '.ts': 'application/typescript',
    '.py': 'text/x-python',
    '.go': 'text/x-go',
    '.rs': 'text/x-rust',
    '.java': 'text/x-java-source',
    '.c': 'text/x-c',
    '.cpp': 'text/x-c++src',
    '.h': 'text/x-chdr',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.gif': 'image/gif',
    '.svg': 'image/svg+xml',
    '.pdf': 'application/pdf',
    '.zip': 'application/zip',
    '.tar': 'application/x-tar',
    '.gz': 'application/gzip',
  };
  const ext = '.' + fileName.split('.').pop()?.toLowerCase();
  return mimeMap[ext] || 'application/octet-stream';
}
