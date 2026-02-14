# Grant Finder MVP

Platform for aggregating, normalizing, and searching funding opportunities (grants, awards, fellowships) using AI and Semantic Search.

## Tech Stack
- **Backend**: Go 1.22+
- **Frontend**: Angular 17+
- **Database**: PostgreSQL 16 + pgvector
- **Infrastructure**: Docker / Cloud Run

## Getting Started

### Prerequisites
- Go 1.22+
- Node.js 20+
- Docker & Docker Compose

### Running Locally

1. **Start Infrastructure**
   ```bash
   docker-compose up -d
   ```

2. **Set Required Environment Variables**
   - `JWT_SECRET` (used for auth token signing)
   - `ADMIN_SECRET` (used for admin ingestion routes)

   PowerShell example:
   ```powershell
   $env:JWT_SECRET="replace-with-strong-secret"
   $env:ADMIN_SECRET="replace-with-strong-secret"
   ```

3. **Run Backend**
   ```bash
   go run cmd/server/main.go
   ```

4. **Run Frontend**
   ```bash
   cd frontend
   npm start
   ```
