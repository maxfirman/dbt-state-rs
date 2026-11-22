//! Live acceptance: the pinned dbt Fusion binary runs a real `dbt build` on
//! Snowflake through the Token broker and the server in `oidc` mode.
//! Opt-in: it skips unless `DBT_STATE_FUSION_SNOWFLAKE_PROFILE` is set, which
//! `scripts/fusion-snowflake.sh` does (see docs/testing.md, "Live Fusion
//! acceptance on Snowflake").
//!
//! The deployment is the harness's TLS test server (gRPC and Token broker
//! listeners, native TLS with the test certificate) with the fake Upstream
//! IdP. The dbt binary is the official pinned release ([`PINNED_DBT`],
//! checked by SHA-256), unpatched, configured only through its supported
//! settings. It runs in an emptied environment with a temporary `HOME` that
//! holds only a copy of one Snowflake profile output, so a developer's dbt
//! platform credential (`~/.dbt/dbt_cloud.yml`, which would select token
//! exchange) is never seen and their own `~/.dbt/state_auth.json` is never
//! written. Every dbt State URL points at the local deployment; hosted dbt
//! State is never contacted.
//!
//! Warehouse safety: the profile output must be a Snowflake output whose
//! database is [`DEMO_DATABASE`]. The run builds a tiny project (one seed,
//! one table) into its own schema, `DBT_STATE_FUSION_<random>`, and drops
//! that schema at the end, also when the test fails.
//!
//! A CI machine builds first with `client_credentials`; then a person builds
//! the same project through Fusion's browser flow. Fusion opens its sign-in
//! URL with `xdg-open` from `PATH`; the harness's `xdg-open` is a shell
//! script that follows the redirects with `curl` (refusing any URL outside
//! the Token broker), as a browser already signed in at the Upstream IdP
//! does, and lands on Fusion's loopback handler. Each build's decisions must
//! be recorded under that build's Principal.

#[path = "support.rs"]
mod support;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::Duration;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use support::fake_idp::{FakeIdp, UpstreamMachine, UpstreamPerson};
use support::oidc::{TestAuth, TestAuthBuilder};
use support::{rows, start_server_tls, TestServer};

/// A JSON file holding one dbt profile output (the `outputs.<target>`
/// mapping of a Snowflake profile), written by `scripts/fusion-snowflake.sh`.
/// Unset (or empty): the test skips.
const PROFILE: &str = "DBT_STATE_FUSION_SNOWFLAKE_PROFILE";

/// The dbt binary to run. It must be [`PINNED_DBT`].
const DBT: &str = "DBT_STATE_FUSION_DBT";

/// The pinned official dbt Fusion release: its version and the SHA-256 of
/// its Linux x86-64 binary, the client this repo's live captures pin (see
/// docs/databricks-append.md).
const PINNED_DBT: (&str, &str) = (
    "dbt 2.0.11",
    "f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31",
);

/// The only Snowflake database the acceptance may write.
const DEMO_DATABASE: &str = "CDO_DEMO_DB";

/// Every acceptance schema starts with this; the cleanup macro refuses any
/// other schema.
const SCHEMA_PREFIX: &str = "DBT_STATE_FUSION_";

/// The Organization both builds use.
const ORGANIZATION: &str = "acceptance";

/// The CI machine's app registration at the Upstream IdP.
const CI: &str = "ci-pipeline";
const CI_SECRET: &str = "ci-secret-Zx81qv";

/// The person who builds second.
const PERSON: &str = "alice-sub";

/// How long one dbt invocation may take.
const DBT_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// CI machines with the `dbt-state.ci` app role, and people in the
/// `data-eng` group, are developers in [`ORGANIZATION`].
fn deployment(idp: &FakeIdp) -> TestAuthBuilder {
    TestAuth::builder(&[(ORGANIZATION, false)])
        .upstream(idp)
        .rule(
            "machine",
            "roles",
            "dbt-state.ci",
            ORGANIZATION,
            "developer",
        )
        .rule("person", "groups", "data-eng", ORGANIZATION, "developer")
}

/// The acceptance's inputs, or `None` (after a message) when it is not
/// enabled.
struct Live {
    dbt: PathBuf,
    output: serde_json::Map<String, Value>,
}

impl Live {
    fn from_env() -> Option<Self> {
        let profile = match std::env::var_os(PROFILE) {
            Some(path) if !path.is_empty() => PathBuf::from(path),
            _ => {
                eprintln!(
                    "SKIPPED: the live Fusion acceptance on Snowflake is opt-in: \
                     run scripts/fusion-snowflake.sh"
                );
                return None;
            }
        };
        let dbt = PathBuf::from(
            std::env::var_os(DBT).unwrap_or_else(|| panic!("{DBT} must name the pinned dbt")),
        );
        let digest = Sha256::digest(std::fs::read(&dbt).expect("read the dbt binary"));
        let digest: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(
            digest,
            PINNED_DBT.1,
            "{} is not the pinned {}",
            dbt.display(),
            PINNED_DBT.0
        );
        let text = std::fs::read_to_string(&profile).expect("read the profile output");
        let output: Value = serde_json::from_str(&text).expect("profile output JSON");
        let Value::Object(output) = output else {
            panic!("the profile output is not a mapping");
        };
        let field = |name: &str| output.get(name).and_then(Value::as_str).unwrap_or_default();
        assert_eq!(field("type"), "snowflake", "not a Snowflake profile output");
        assert!(
            field("database").eq_ignore_ascii_case(DEMO_DATABASE),
            "the profile output's database is not {DEMO_DATABASE}"
        );
        Some(Self { dbt, output })
    }
}

/// The tiny dbt project, its acceptance schema and the CA bundle dbt
/// trusts: the host's roots (Snowflake) and the test CA (the deployment).
struct Project {
    dir: tempfile::TempDir,
    schema: String,
    dbt: PathBuf,
    output: serde_json::Map<String, Value>,
    built: bool,
}

const PROJECT_YML: &str = "\
name: fusion_acceptance
version: '1.0'
config-version: 2
profile: snowflake
seed-paths: [seeds]
model-paths: [models]
macro-paths: [macros]
";

const SEED_CSV: &str = "id,name\n1,ada\n2,grace\n3,edsger\n";

const MODEL_SQL: &str = "\
{{ config(materialized='table') }}
select count(*) as people from {{ ref('acceptance_people') }}
";

/// Drops the run's own schema, refusing any other database or schema.
const CLEANUP_MACRO: &str = "\
{% macro drop_acceptance_schema() %}
  {% if target.type != 'snowflake' or (target.database | upper) != 'CDO_DEMO_DB'
        or not (target.schema | upper).startswith('DBT_STATE_FUSION_') %}
    {{ exceptions.raise_compiler_error('refusing to drop ' ~ target.database ~ '.' ~ target.schema) }}
  {% endif %}
  {% do run_query('drop schema if exists ' ~ target.database ~ '.' ~ target.schema) %}
{% endmacro %}
";

impl Project {
    fn new(live: &Live) -> Self {
        let dir = tempfile::tempdir().expect("project dir");
        let root = dir.path().join("project");
        for (path, text) in [
            ("dbt_project.yml", PROJECT_YML),
            ("seeds/acceptance_people.csv", SEED_CSV),
            ("models/acceptance_summary.sql", MODEL_SQL),
            ("macros/drop_acceptance_schema.sql", CLEANUP_MACRO),
        ] {
            let path = root.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).expect("project dirs");
            std::fs::write(path, text).expect("project file");
        }
        let host_roots = std::env::var_os("SSL_CERT_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|| "/etc/ssl/certs/ca-certificates.crt".into());
        let mut bundle = std::fs::read(&host_roots).expect("read the host's CA bundle");
        bundle.push(b'\n');
        bundle.extend(std::fs::read(support::tls::ca_file()).expect("read the test CA"));
        std::fs::write(dir.path().join("ca-bundle.pem"), bundle).expect("write the CA bundle");
        let schema = format!(
            "{SCHEMA_PREFIX}{}",
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        )
        .to_uppercase();
        Self {
            dir,
            schema,
            dbt: live.dbt.clone(),
            output: live.output.clone(),
            built: false,
        }
    }

    fn root(&self) -> PathBuf {
        self.dir.path().join("project")
    }

    fn ca_bundle(&self) -> PathBuf {
        self.dir.path().join("ca-bundle.pem")
    }

    /// A fresh `HOME` holding only `.dbt/profiles.yml`: the copied profile
    /// output, in the acceptance schema, as profile `snowflake`, target
    /// `cdo_demo`.
    fn home(&self) -> tempfile::TempDir {
        let home = tempfile::tempdir().expect("dbt home");
        let dbt_dir = home.path().join(".dbt");
        std::fs::create_dir(&dbt_dir).expect(".dbt");
        let mut output = self.output.clone();
        output.insert("schema".into(), json!(self.schema));
        output.insert("threads".into(), json!(1));
        // JSON is YAML.
        let profiles =
            json!({"snowflake": {"target": "cdo_demo", "outputs": {"cdo_demo": output}}});
        let path = dbt_dir.join("profiles.yml");
        std::fs::write(&path, profiles.to_string()).expect("write profiles.yml");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("profiles.yml mode");
        home
    }

    /// A `dbt` command for `home`, in an emptied environment: `PATH`, `HOME`,
    /// the CA bundle and proxy settings, telemetry off, and every dbt State
    /// URL naming `api` (gRPC, `host:port`) and `broker`, so hosted dbt
    /// State is never contacted. With a `desktop`, dbt runs as a person at a
    /// terminal: in a pseudo-terminal (util-linux `script`; Fusion's browser
    /// flow needs an interactive terminal), with the desktop's URL opener
    /// first on `PATH`.
    fn command(
        &self,
        home: &Path,
        api: &str,
        broker: &str,
        desktop: Option<&Desktop>,
        args: &[&str],
    ) -> std::process::Command {
        let mut dbt = vec![self.dbt.to_str().expect("UTF-8 path").to_owned()];
        dbt.extend(args.iter().map(|arg| (*arg).to_owned()));
        dbt.push("--project-dir".into());
        dbt.push(self.root().to_str().expect("UTF-8 path").into());
        dbt.push("--profiles-dir".into());
        dbt.push(home.join(".dbt").to_str().expect("UTF-8 path").into());
        let mut command = match desktop {
            None => {
                let mut command = std::process::Command::new(&dbt[0]);
                command.args(&dbt[1..]);
                command
            }
            Some(_) => {
                let line: Vec<String> = dbt
                    .iter()
                    .map(|arg| format!("'{}'", arg.replace('\'', r"'\''")))
                    .collect();
                let mut command = std::process::Command::new("script");
                command
                    .args(["--quiet", "--return", "--command"])
                    .arg(line.join(" "))
                    .arg("/dev/null");
                command
            }
        };
        command.current_dir(self.root()).env_clear();
        let path = std::env::var_os("PATH").unwrap_or_default();
        let path = std::env::join_paths(
            desktop
                .map(|desktop| desktop.path().to_path_buf())
                .into_iter()
                .chain(std::env::split_paths(&path)),
        )
        .expect("PATH");
        command.env("PATH", path);
        for key in [
            "TMPDIR",
            "LANG",
            "HTTPS_PROXY",
            "https_proxy",
            "HTTP_PROXY",
            "http_proxy",
        ] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        let no_proxy = match std::env::var("NO_PROXY") {
            Ok(value) if !value.is_empty() => format!("{value},127.0.0.1,localhost"),
            _ => "127.0.0.1,localhost".into(),
        };
        let token = format!("{broker}/token");
        command
            .env("HOME", home)
            .env("SSL_CERT_FILE", self.ca_bundle())
            .env("NO_PROXY", &no_proxy)
            .env("no_proxy", &no_proxy)
            .env("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
            .env("DO_NOT_TRACK", "1")
            .env("RUN_CACHE_API_URL", api)
            .env("DBT_ENGINE_STATE_API_URL", api)
            .env("RUN_CACHE_API_SECURE", "true")
            .env("RUN_CACHE_AUTH_URL", broker)
            .env("DBT_ENGINE_STATE_AUTH_URL", broker)
            .env("RUN_CACHE_TOKEN_URL", &token)
            .env("DBT_ENGINE_STATE_TOKEN_URL", &token);
        command
    }

    /// A `dbt build` with dbt State on, against `server`.
    fn build(
        &self,
        server: &TestServer,
        home: &Path,
        desktop: Option<&Desktop>,
    ) -> tokio::process::Command {
        let mut command = self.command(
            home,
            &server.addr.to_string(),
            &server.broker_url(),
            desktop,
            &["build"],
        );
        command.env("DBT_ENGINE_MANAGE_STATE", "true");
        let mut command = tokio::process::Command::from(command);
        command.kill_on_drop(true);
        command
    }

    /// Run `command` to its end within [`DBT_TIMEOUT`].
    async fn run(mut command: tokio::process::Command) -> Output {
        tokio::time::timeout(DBT_TIMEOUT, command.output())
            .await
            .expect("dbt finished in time")
            .expect("run dbt")
    }
}

impl Drop for Project {
    /// Drop the acceptance schema with dbt State off, in a fresh `HOME`.
    fn drop(&mut self) {
        if !self.built {
            return;
        }
        let home = self.home();
        // dbt State off, and its URLs at a closed loopback port all the same.
        let status = self
            .command(
                home.path(),
                "127.0.0.1:9",
                "https://127.0.0.1:9",
                None,
                &["run-operation", "drop_acceptance_schema"],
            )
            .env("DBT_ENGINE_MANAGE_STATE", "false")
            .status();
        match status {
            Ok(status) if status.success() => {
                eprintln!("dropped {DEMO_DATABASE}.{}", self.schema)
            }
            other => eprintln!(
                "FAILED to drop {DEMO_DATABASE}.{}: {other:?}; drop it by hand",
                self.schema
            ),
        }
    }
}

fn describe(output: &Output) -> String {
    format!(
        "status {}\n--- stdout\n{}\n--- stderr\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// Fusion's loopback handler: its redirect URI, without query.
const LOOPBACK_HANDLER: &str = "http://127.0.0.1:29525/handler";

/// A desktop whose URL opener (`xdg-open`, first on `PATH`) is a browser
/// already signed in at the Upstream IdP: it follows every redirect from a
/// URL under the Token broker to the end (Fusion's loopback handler) with
/// `curl`, and logs where it started and landed, without queries (they
/// carry the authorization code). It refuses any other URL.
struct Desktop {
    dir: tempfile::TempDir,
}

impl Desktop {
    fn new(broker: &str, ca_bundle: &Path) -> Self {
        let dir = tempfile::tempdir().expect("desktop");
        let quote = |text: &str| format!("'{}'", text.replace('\'', r"'\''"));
        let log = dir.path().join("visits.log");
        let script = format!(
            "#!/bin/sh\n\
             broker={broker}\nlog={log}\nca={ca}\n\
             case \"$1\" in\n\
             \"$broker\" | \"$broker\"/* | \"$broker\"\\?*) ;;\n\
             *) echo \"refused ${{1%%\\?*}}\" >>\"$log\"; exit 1 ;;\n\
             esac\n\
             echo \"opened ${{1%%\\?*}}\" >>\"$log\"\n\
             landed=$(curl --silent --show-error --location --max-redirs 10 \
               --cacert \"$ca\" --noproxy '*' --output /dev/null \
               --write-out '%{{http_code}} %{{url_effective}}' \"$1\" 2>>\"$log\")\n\
             url=${{landed#* }}\n\
             echo \"landed ${{url%%\\?*}} ${{landed%% *}}\" >>\"$log\"\n",
            broker = quote(broker),
            log = quote(log.to_str().expect("UTF-8 path")),
            ca = quote(ca_bundle.to_str().expect("UTF-8 path")),
        );
        let opener = dir.path().join("xdg-open");
        std::fs::write(&opener, script).expect("write xdg-open");
        std::fs::set_permissions(&opener, std::fs::Permissions::from_mode(0o755))
            .expect("make xdg-open executable");
        Self { dir }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    /// The browser's log lines so far.
    fn visits(&self) -> Vec<String> {
        match std::fs::read_to_string(self.dir.path().join("visits.log")) {
            Ok(text) => text.lines().map(str::to_owned).collect(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => panic!("read the browser log: {error}"),
        }
    }
}

/// The Principal recorded for `subject`.
async fn principal(server: &TestServer, subject: &str) -> rows::StoredPrincipal {
    let principals = rows::principals(server).await;
    principals
        .into_iter()
        .find(|principal| principal.subject == subject)
        .unwrap_or_else(|| panic!("no Principal for {subject}"))
}

/// The machine builds with `client_credentials`, then the person builds
/// the same project through the browser flow; each build's decisions are
/// recorded in [`ORGANIZATION`] under its own Principal, and the person's
/// build reuses what the machine built.
#[tokio::test(flavor = "multi_thread")]
async fn fusion_builds_on_snowflake_as_a_machine_and_as_a_person() {
    let Some(live) = Live::from_env() else {
        return;
    };
    let idp = FakeIdp::start().await;
    idp.machine(UpstreamMachine::new(CI, CI_SECRET).claim("roles", json!(["dbt-state.ci"])));
    idp.sign_in(UpstreamPerson::new(PERSON).claim("groups", json!(["data-eng"])));
    let server = start_server_tls(deployment(&idp)).await;
    let mut project = Project::new(&live);

    // A CI machine: client_credentials, no browser.
    let machine_home = project.home();
    let mut command = project.build(&server, machine_home.path(), None);
    command
        .env("DBT_ENGINE_STATE_OAUTH_CLIENT_ID", CI)
        .env("DBT_ENV_SECRET_STATE_OAUTH_CLIENT_SECRET", CI_SECRET);
    project.built = true;
    let machine = Project::run(command).await;
    eprintln!("machine build:\n{}", describe(&machine));
    assert!(machine.status.success(), "{}", describe(&machine));
    let machine_decisions = rows::decisions(&server, 2).await;
    let ci = principal(&server, &format!("sp-{CI}")).await;
    assert_eq!(
        (&*ci.issuer, &*ci.kind),
        (&*idp.issuer(), "machine"),
        "{ci:?}"
    );
    assert_eq!(machine_decisions.len(), 2, "{machine_decisions:?}");
    for decision in &machine_decisions {
        assert_eq!(decision.org_id, ORGANIZATION, "{decision:?}");
        assert_eq!(decision.decision, "build", "{decision:?}");
        assert_eq!(
            decision.principal_id.as_deref(),
            Some(&*ci.id),
            "{decision:?}"
        );
    }
    assert!(
        !machine_home.path().join(".dbt/state_auth.json").exists(),
        "a machine keeps its token in memory only"
    );

    // A person: the browser flow, through the Token broker and the
    // Upstream IdP, to Fusion's loopback handler.
    let _loopback = support::client_driver::hold_loopback_port().await;
    let person_home = project.home();
    let desktop = Desktop::new(&server.broker_url(), &project.ca_bundle());
    let person = Project::run(project.build(&server, person_home.path(), Some(&desktop))).await;
    eprintln!("person build:\n{}", describe(&person));
    assert!(person.status.success(), "{}", describe(&person));
    assert_eq!(
        desktop.visits(),
        [
            format!("opened {}/", server.broker_url()),
            format!("landed {LOOPBACK_HANDLER} 200"),
        ],
        "one sign-in, ending at Fusion's loopback handler"
    );
    assert!(
        person_home.path().join(".dbt/state_auth.json").is_file(),
        "Fusion keeps a person's token in their home"
    );
    let decisions = rows::decisions(&server, 4).await;
    let alice = principal(&server, PERSON).await;
    assert_eq!(
        (&*alice.issuer, &*alice.kind),
        (&*idp.issuer(), "person"),
        "{alice:?}"
    );
    assert_eq!(rows::principals(&server).await.len(), 2);
    let person_decisions = &decisions[machine_decisions.len()..];
    assert_eq!(person_decisions.len(), 2, "{decisions:?}");
    for decision in person_decisions {
        assert_eq!(decision.org_id, ORGANIZATION, "{decision:?}");
        assert_eq!(decision.decision, "skip", "{decision:?}");
        assert_eq!(
            decision.principal_id.as_deref(),
            Some(&*alice.id),
            "{decision:?}"
        );
    }
}
