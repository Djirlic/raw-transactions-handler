import tomllib
from pathlib import Path


def main():
    pyproject_path = Path("pyproject.toml")
    output_path = Path("requirements-layer.txt")

    with pyproject_path.open("rb") as f:
        data = tomllib.load(f)

    deps = data.get("project", {}).get("dependencies", [])
    output_path.write_text("\n".join(deps) + "\n")
    print(f"Wrote {len(deps)} dependencies to {output_path}")


if __name__ == "__main__":
    main()
