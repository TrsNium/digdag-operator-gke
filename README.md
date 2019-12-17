# digdag-operator-gke
[![Jitpack](https://jitpack.io/v/myui/digdag-plugin-example.svg)](https://jitpack.io/#myui/digdag-plugin-example) [![Digdag](https://img.shields.io/badge/digdag-v0.9.12-brightgreen.svg)](https://github.com/treasure-data/digdag/releases/tag/v0.9.12)

# 1) build

```sh
./gradlew publish
```

Artifacts are build on local repos: `./build/repo`.

# 2) run an example

```sh
digdag selfupdate

digdag run --project sample plugin.dig -p repos=`pwd`/build/repo
```

You'll find the result of the task in `./sample/example.out`.
