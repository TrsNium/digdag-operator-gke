# digdag-operator-gke

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

