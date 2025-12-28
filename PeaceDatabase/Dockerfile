FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Restore dependencies for all solution projects
COPY PeaceDatabase/PeaceDatabase.sln PeaceDatabase/
COPY PeaceDatabase/PeaceDatabase.csproj PeaceDatabase/
COPY PeaceDatabase.Tests/PeaceDatabase.Tests.csproj PeaceDatabase.Tests/
COPY PeaceDatabase.Gzip/PeaceDatabase.Gzip.csproj PeaceDatabase.Gzip/
RUN dotnet restore PeaceDatabase/PeaceDatabase.sln

# Build and publish the main service
COPY . .
WORKDIR /src
RUN dotnet publish PeaceDatabase/PeaceDatabase.csproj -c Release -o /app/publish

# Runtime image
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app

# Lightweight HTTP health probing
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/publish .

ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --retries=5 CMD curl -f http://localhost:8080/healthz || exit 1

ENTRYPOINT ["dotnet", "PeaceDatabase.dll"]
