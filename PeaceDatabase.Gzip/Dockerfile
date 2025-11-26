# GZIP Tool Dockerfile
# Build: docker build -t peacedb-gzip -f PeaceDatabase.Gzip/Dockerfile .
# Usage: docker run -v $(pwd):/data peacedb-gzip compress /data/input.txt /data/output.gz

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Copy project files
COPY PeaceDatabase/PeaceDatabase.csproj PeaceDatabase/
COPY PeaceDatabase.Gzip/PeaceDatabase.Gzip.csproj PeaceDatabase.Gzip/
RUN dotnet restore PeaceDatabase.Gzip/PeaceDatabase.Gzip.csproj

# Copy source code
COPY PeaceDatabase/ PeaceDatabase/
COPY PeaceDatabase.Gzip/ PeaceDatabase.Gzip/

# Build
WORKDIR /src/PeaceDatabase.Gzip
RUN dotnet publish -c Release -o /app/publish

# Runtime image
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "PeaceDatabase.Gzip.dll"]

