#!/usr/bin/env bash
#
# Build the new-api image and package a loadable offline bundle for the
# new-api + Langfuse v4 stack in deploy/langfuse/.
#
# The bundle contains everything needed on an air-gapped host: compose file,
# env template, postgres initdb script, README, every stack image (images.tar),
# and a load.sh helper.
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
ENV_EXAMPLE="$SCRIPT_DIR/.env.example"

usage() {
  cat <<'EOF'
Usage: package.sh [options]

Build the new-api image and produce new-api-langfuse-<version>.tar.gz, a
self-contained offline bundle for the new-api + Langfuse v4 stack.

Options:
  --no-image        Skip `docker build` (reuse the existing image for --tag).
  --no-save         Skip `docker save` (bundle without images.tar).
  --output DIR      Output directory for the archive (default: deploy/langfuse/dist).
  --tag TAG         Image tag to build and ship
                    (default: new-api:langfuse-<VERSION>).
  -h, --help        Show this help.

The version comes from the VERSION environment variable, else a non-empty
VERSION file, else `git describe --tags --always` (a leading "v" is stripped).
The resolved value is written to VERSION for the build (the Dockerfile embeds it
into the binary and the frontend) and the original file is restored afterwards.

Images that are not present locally are pulled with `docker pull`; if a pull
fails the script stops and names the missing image.
EOF
}

log() {
  printf '==> %s\n' "$*"
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

BUILD_IMAGE=1
SAVE_IMAGES=1
OUTPUT_DIR="$SCRIPT_DIR/dist"
TAG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      usage
      exit 0
      ;;
    --no-image)
      BUILD_IMAGE=0
      shift
      ;;
    --no-save)
      SAVE_IMAGES=0
      shift
      ;;
    --output)
      [[ $# -ge 2 && -n "$2" ]] || die "--output requires a directory argument"
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --tag)
      [[ $# -ge 2 && -n "$2" ]] || die "--tag requires an image tag argument"
      TAG="$2"
      shift 2
      ;;
    *)
      printf 'ERROR: unknown option %s\n\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

command -v docker >/dev/null 2>&1 || die "docker not found in PATH"
docker compose version >/dev/null 2>&1 || die "the docker compose plugin is required (docker compose version failed)"

[[ -f "$COMPOSE_FILE" ]] || die "missing $COMPOSE_FILE"
[[ -f "$ENV_EXAMPLE" ]] || die "missing $ENV_EXAMPLE"
[[ -f "$SCRIPT_DIR/README.md" ]] || die "missing $SCRIPT_DIR/README.md"
[[ -d "$SCRIPT_DIR/initdb" ]] || die "missing $SCRIPT_DIR/initdb/"

# The repository's VERSION file is intentionally empty in git; CI writes it
# before building. Resolve a real version so the Dockerfile can embed it into
# the Go binary and the frontend: $VERSION env, then a non-empty VERSION file,
# then `git describe --tags --always`.
VERSION_FILE="$REPO_ROOT/VERSION"
VERSION_FROM_ENV="${VERSION:-}"
VERSION=""
if [[ -n "$VERSION_FROM_ENV" ]]; then
  VERSION="$VERSION_FROM_ENV"
elif [[ -r "$VERSION_FILE" && -n "$(tr -d '[:space:]' <"$VERSION_FILE")" ]]; then
  VERSION=$(tr -d '[:space:]' <"$VERSION_FILE")
elif command -v git >/dev/null 2>&1 && git -C "$REPO_ROOT" rev-parse --git-dir >/dev/null 2>&1; then
  VERSION=$(git -C "$REPO_ROOT" describe --tags --always 2>/dev/null || true)
fi
VERSION="${VERSION#v}"
[[ -n "$VERSION" ]] ||
  die "cannot resolve a version: set VERSION=<value>, or ensure $VERSION_FILE is non-empty or git describe works here"

if [[ -z "$TAG" ]]; then
  TAG="new-api:langfuse-$VERSION"
fi

mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR=$(cd -- "$OUTPUT_DIR" && pwd)
ARCHIVE="$OUTPUT_DIR/new-api-langfuse-$VERSION.tar.gz"
STAGE_DIR=$(mktemp -d "${TMPDIR:-/tmp}/new-api-langfuse.XXXXXX")
# Kept outside STAGE_DIR so the restored file never ends up in the archive.
VERSION_BACKUP=$(mktemp "${TMPDIR:-/tmp}/new-api-version.XXXXXX")

# VERSION was possibly empty (or absent) before the build; restore it on exit.
VERSION_EXISTED=0
if [[ -f "$VERSION_FILE" ]]; then
  VERSION_EXISTED=1
  cp -- "$VERSION_FILE" "$VERSION_BACKUP"
fi
VERSION_WRITTEN=0
cleanup() {
  if [[ $VERSION_WRITTEN -eq 1 ]]; then
    if [[ $VERSION_EXISTED -eq 1 ]]; then
      cp -- "$VERSION_BACKUP" "$VERSION_FILE"
    else
      rm -f "$VERSION_FILE"
    fi
  fi
  if [[ -n "${VERSION_BACKUP:-}" ]]; then
    rm -f "$VERSION_BACKUP"
  fi
  if [[ -n "${STAGE_DIR:-}" ]]; then
    rm -rf "$STAGE_DIR"
  fi
}
trap cleanup EXIT

log "Validating $COMPOSE_FILE"
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_EXAMPLE" config -q

if [[ $BUILD_IMAGE -eq 1 ]]; then
  log "Writing version '$VERSION' to $VERSION_FILE for the build"
  printf '%s\n' "$VERSION" >"$VERSION_FILE"
  VERSION_WRITTEN=1
  log "Building image $TAG from $REPO_ROOT"
  docker build -f "$REPO_ROOT/Dockerfile" -t "$TAG" "$REPO_ROOT"
else
  log "Skipping image build (--no-image); expecting $TAG to exist locally"
fi

ensure_image() {
  local image=$1
  if docker image inspect "$image" >/dev/null 2>&1; then
    return 0
  fi
  log "Image $image is missing locally; pulling"
  if ! docker pull "$image" >&2; then
    die "image '$image' is not available locally and 'docker pull $image' failed. Pull it on a networked machine, or override the matching *_IMAGE variable to a reachable mirror."
  fi
}

IMAGES=()
if [[ $SAVE_IMAGES -eq 1 ]]; then
  # NEW_API_IMAGE is overridden so compose resolves the image we just built
  # instead of the template default from .env.example.
  while IFS= read -r image; do
    [[ -n "$image" ]] && IMAGES+=("$image")
  done < <(NEW_API_IMAGE="$TAG" docker compose -f "$COMPOSE_FILE" --env-file "$ENV_EXAMPLE" config --images | sort -u)

  [[ ${#IMAGES[@]} -gt 0 ]] || die "could not determine the stack image list from docker compose"

  log "Collecting ${#IMAGES[@]} images"
  for image in "${IMAGES[@]}"; do
    if [[ "$image" == "$TAG" ]]; then
      docker image inspect "$image" >/dev/null 2>&1 ||
        die "image '$image' not found locally; build it (drop --no-image) or pass --tag for an existing image"
    else
      ensure_image "$image"
    fi
  done

  log "Saving images to images.tar"
  docker save -o "$STAGE_DIR/images.tar" "${IMAGES[@]}"
else
  log "Skipping image save (--no-save)"
fi

log "Staging bundle files"
cp "$COMPOSE_FILE" "$ENV_EXAMPLE" "$SCRIPT_DIR/README.md" "$STAGE_DIR/"
cp -R "$SCRIPT_DIR/initdb" "$STAGE_DIR/initdb"

cat >"$STAGE_DIR/load.sh" <<'LOAD_SCRIPT'
#!/usr/bin/env bash
# Generated by deploy/langfuse/package.sh — load this bundle and print next steps.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

if [[ -f images.tar ]]; then
  echo "==> Loading images from images.tar"
  docker load -i images.tar
else
  echo "WARNING: images.tar is missing; the stack images must already be present." >&2
fi

if [[ ! -f .env ]]; then
  cp .env.example .env
  if grep -q '^NEW_API_IMAGE=' .env; then
    sed -i.bak "s|^NEW_API_IMAGE=.*|NEW_API_IMAGE=@TAG@|" .env
    rm -f .env.bak
  else
    printf '\nNEW_API_IMAGE=@TAG@\n' >>.env
  fi
  echo "==> Created .env from .env.example and set NEW_API_IMAGE=@TAG@"
  echo "    Replace every CHANGEME value in .env before starting."
else
  echo "==> .env already exists; leaving it untouched"
fi

cat <<'NEXT'
Next steps:
  1. Edit .env and replace every CHANGEME secret (see the comments in the file).
  2. docker compose up -d
  3. Langfuse UI:  http://localhost:3001  (sign in with LANGFUSE_INIT_USER_EMAIL/PASSWORD)
     new-api UI:   http://localhost:3000
NEXT
LOAD_SCRIPT
sed -i "s|@TAG@|$TAG|g" "$STAGE_DIR/load.sh"
chmod +x "$STAGE_DIR/load.sh"

log "Creating $ARCHIVE"
tar -czf "$ARCHIVE" -C "$STAGE_DIR" .

SIZE=$(du -h "$ARCHIVE" | cut -f1)

cat <<EOF

Bundle ready: $ARCHIVE ($SIZE)
  Image tag:  $TAG
  Contents:   docker-compose.yml, .env.example, initdb/, README.md, load.sh$( [[ $SAVE_IMAGES -eq 1 ]] && printf ', images.tar' )

On the target host:
  tar -xzf $(basename "$ARCHIVE") -C /opt/new-api-langfuse
  cd /opt/new-api-langfuse
  docker load -i images.tar
  cp .env.example .env && \$EDITOR .env
  docker compose up -d

Or run the bundled helper, which loads images and creates .env when missing:
  ./load.sh && docker compose up -d
EOF
