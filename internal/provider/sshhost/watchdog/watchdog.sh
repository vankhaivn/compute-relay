#!/bin/sh
# Frozen installation material. No configuration file is sourced or evaluated.
set -f
umask 077
PATH=@@PATH@@
LC_ALL=C
export PATH LC_ALL
unset DOCKER_CONTEXT DOCKER_HOST DOCKER_CONFIG DOCKER_TLS_VERIFY DOCKER_CERT_PATH DOCKER_API_VERSION ENV BASH_ENV CDPATH PERL5OPT PERL5LIB PERLLIB
root=@@DIRECTORY@@
installation=@@INSTALLATION@@
docker=@@DOCKER@@
docker_host=@@DOCKER_HOST@@
engine=@@ENGINE@@
thermal_threshold=@@THERMAL@@
speed_threshold=@@CPU_SPEED@@
temperature_threshold=@@TEMPERATURE@@
critical_duration=@@DURATION@@
low_battery=@@BATTERY@@
interval=@@INTERVAL@@
powermetrics_granted=@@POWERMETRICS@@
source_digest=@@SOURCE_DIGEST@@
sysctl=@@SYSCTL@@
pmset=@@PMSET@@
sudo=@@SUDO@@
uname=@@UNAME@@
date=@@DATE@@
stat=@@STAT@@
id=@@ID@@
sample_sleep=@@SLEEP@@
flush=@@SYNC@@
expected_manifest=${1-}
script=$root/watchdog-v1.sh
manifest=$root/manifest-v1.txt
status=$root/status-v1.json
ledger=$root/stop-ledger-v1.txt
work=$root/.command-v1.out
status_tmp=$root/.status-v1.tmp
ledger_tmp=$root/.stop-ledger-v1.tmp
ledger_header='compute-relay-watchdog-stops-v1 host_critical_stopped'
child=
timer=

regular_or_missing() {
    [ ! -L "$1" ] && { [ ! -e "$1" ] || [ -f "$1" ]; }
}

write_private() {
    regular_or_missing "$1" || return 1
    /bin/rm -f "$1" || return 1
    (set -C; printf '%s\n' "$2" >"$1")
}

# Every launched command has its own direct PID, finite output file and timer.
# No process group or PID obtained from host output is ever signalled.
capture() {
    cap=$1
    timeout=$2
    shift 2
    command_output=
    regular_or_missing "$work" || return 1
    /bin/rm -f "$work" || return 1
    (set -C; ulimit -f 16; exec "$@" >"$work") 2>/dev/null &
    child=$!
    (
        delay=
        trap 'if [ -n "$delay" ]; then kill -TERM "$delay" 2>/dev/null; wait "$delay" 2>/dev/null; fi; exit 0' TERM INT HUP
        /bin/sleep "$timeout" &
        delay=$!
        wait "$delay" || exit 0
        delay=
        kill -TERM "$child" 2>/dev/null || exit 0
        /bin/sleep 1 &
        delay=$!
        wait "$delay" || exit 0
        delay=
        kill -KILL "$child" 2>/dev/null
    ) >/dev/null 2>&1 &
    timer=$!
    wait "$child" 2>/dev/null
    code=$?
    child=
    kill -TERM "$timer" 2>/dev/null
    wait "$timer" 2>/dev/null
    timer=
    [ "$code" = 0 ] || return 1
    bytes=$(/usr/bin/wc -c <"$work") || return 1
    [ "$bytes" -le "$cap" ] 2>/dev/null || return 1
    command_output=$(/bin/cat "$work") || return 1
}

cleanup() {
    trap '' TERM INT HUP
    if [ -n "$child" ]; then
        kill -TERM "$child" 2>/dev/null
        /bin/sleep 1
        kill -KILL "$child" 2>/dev/null
        wait "$child" 2>/dev/null
    fi
    if [ -n "$timer" ]; then kill -TERM "$timer" 2>/dev/null; wait "$timer" 2>/dev/null; fi
    /bin/rm -f "$work" "$status_tmp" "$ledger_tmp"
}
trap 'cleanup' EXIT
trap 'exit 0' TERM INT HUP

write_status() {
    action=$1
    capture 64 3 "$date" -u '+%Y-%m-%dT%H:%M:%SZ' || return 1
    timestamp=$command_output
    printf '%s\n' "$timestamp" | /usr/bin/awk 'NR != 1 || $0 !~ /^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]Z$/ { bad=1 } END { exit bad }' || return 1
    regular_or_missing "$status" && regular_or_missing "$status_tmp" || return 1
    write_private "$status_tmp" "{\"version\":1,\"last_run\":\"$timestamp\",\"last_action\":\"$action\"}" || return 1
    /bin/mv -f "$status_tmp" "$status"
}

integrity() {
    [ -f "$script" ] && [ ! -L "$script" ] && [ -f "$manifest" ] && [ ! -L "$manifest" ] || return 1
    capture 4096 3 /usr/bin/shasum -a 256 "$manifest" || return 1
    [ "$command_output" = "$expected_manifest  $manifest" ] || return 1
    capture 512 3 /bin/cat "$manifest" || return 1
    manifest_body=$command_output
    manifest_script=$(printf '%s\n' "$manifest_body" | /usr/bin/awk -v source="$source_digest" '
        NR==1 && $0!="compute-relay-host-watchdog 1" {bad=1}
        NR==2 && $0!="source-sha256 " source {bad=1}
        NR==3 {if ($1!="script-sha256" || NF!=2 || length($2)!=64 || $2!~/^[a-f0-9]+$/) bad=1; value=$2}
        END {if (bad || NR!=3) exit 1; print value}') || return 1
    capture 4096 3 /usr/bin/shasum -a 256 "$script" || return 1
    [ "$command_output" = "$manifest_script  $script" ]
}

epoch() {
    capture 32 3 "$date" -u '+%s' || return 1
    printf '%s\n' "$command_output" | /usr/bin/awk 'NR!=1 || $0!~/^[1-9][0-9]*$/ || length($0)>10 {bad=1} END {exit bad}' || return 1
    now=$command_output
}

fresh_sample() {
    epoch || return 1
    [ "$now" -ge "$sample_start" ] && [ "$((now - sample_start))" -le "$max_sample_age" ]
}

load_ledger() {
    stopped_ids=$ledger_header
    stopped_count=0
    [ -e "$ledger" ] || { [ ! -L "$ledger" ]; return; }
    [ -f "$ledger" ] && [ ! -L "$ledger" ] || return 1
    capture 8192 3 /bin/cat "$ledger" || return 1
    stopped_ids=$command_output
    stopped_count=$(printf '%s\n' "$stopped_ids" | /usr/bin/awk -v header="$ledger_header" '
        NR==1 {if ($0!=header) bad=1; next}
        {if (length($0)!=64 || $0!~/^[a-f0-9]+$/ || seen[$0]++) bad=1}
        END {if (bad || NR>65) exit 1; print NR-1}') || return 1
}

was_stopped() {
    case "
$stopped_ids
" in *"
$1
"*) return 0;; *) return 1;; esac
}

# Docker output is data only. Each inspected ID first came from this filtered
# list. Ownership labels are required again, including the frozen daemon ID.
scan_owned() {
    fresh_sample || return 1
    capture 256 8 "$docker" --host "$docker_host" info --format '{{.ID}}' || return 1
    [ "$command_output" = "$engine" ] || return 1
    fresh_sample || return 1
    capture 8192 8 "$docker" --host "$docker_host" container ls --quiet --no-trunc --filter status=running \
        --filter "label=io.compute-relay.installation=$installation" \
        --filter label=io.compute-relay.instance --filter label=io.compute-relay.attempt \
        --filter label=io.compute-relay.preparation --filter label=io.compute-relay.workspace \
        --filter label=io.compute-relay.job --filter label=io.compute-relay.nonce \
        --filter label=io.compute-relay.plan --filter label=io.compute-relay.engine || return 1
    owned_ids=$command_output
    [ -n "$owned_ids" ] || return 0
    printf '%s\n' "$owned_ids" | /usr/bin/awk '
        {if (length($0)!=64 || $0!~/^[a-f0-9]+$/ || seen[$0]++) bad=1}
        END {exit bad || NR>64}' || return 1
    for candidate in $owned_ids; do
        fresh_sample || return 1
        inspect_owned "$candidate" || return 1
        [ "$running" = true ] && [ "$container_status" = running ] || return 1
    done
    fresh_sample
}

inspect_owned() {
    wanted=$1
    capture 4096 8 "$docker" --host "$docker_host" container inspect --format \
        '{{println .Id}}{{println (index .Config.Labels "io.compute-relay.installation")}}{{println (index .Config.Labels "io.compute-relay.instance")}}{{println (index .Config.Labels "io.compute-relay.attempt")}}{{println (index .Config.Labels "io.compute-relay.preparation")}}{{println (index .Config.Labels "io.compute-relay.workspace")}}{{println (index .Config.Labels "io.compute-relay.job")}}{{println (index .Config.Labels "io.compute-relay.nonce")}}{{println (index .Config.Labels "io.compute-relay.plan")}}{{println (index .Config.Labels "io.compute-relay.engine")}}{{println .State.Running}}{{println .State.Status}}relay-watchdog-v1-end' \
        -- "$wanted" || return 1
    inspected=$(printf '%s\n' "$command_output" | /usr/bin/awk -v wanted="$wanted" -v installation="$installation" -v engine="$engine" '
        function opaque(s) {return length(s)>0 && length(s)<=128 && s~/^[A-Za-z0-9][A-Za-z0-9_.:-]*$/}
        NR==1 && ("x" $0)!=("x" wanted) {bad=1}
        NR==2 && ("x" $0)!=("x" installation) {bad=1}
        NR>=3 && NR<=8 && !opaque($0) {bad=1}
        NR==8 && length($0)<16 {bad=1}
        NR==9 && (length($0)!=64 || $0!~/^[a-f0-9]+$/) {bad=1}
        NR==10 && ("x" $0)!=("x" engine) {bad=1}
        NR==11 {if ($0!="true" && $0!="false") bad=1; value=$0}
        NR==12 {if ($0!~/^(created|running|paused|restarting|removing|exited|dead)$/) bad=1; status=$0}
        NR==13 && $0!="relay-watchdog-v1-end" {bad=1}
        END {if (bad || NR!=13) exit 1; print value " " status}') || return 1
    running=${inspected%% *}
    container_status=${inspected#* }
}

stop_owned() {
    load_ledger || { result=attention_required; return; }
    scan_owned || { if [ "$stopped_count" -gt 0 ]; then result=stop_unknown; else result=unknown; fi; return; }
    pending=0
    [ "$stopped_count" = 0 ] || pending=1
    new_count=0
    updated_ledger=$stopped_ids
    set --
    for candidate in $owned_ids; do
        if was_stopped "$candidate"; then
            pending=1
        else
            set -- "$@" "$candidate"
            new_count=$((new_count + 1))
            updated_ledger="$updated_ledger
$candidate"
        fi
    done
    if [ "$new_count" = 0 ]; then
        if [ "$stopped_count" -gt 0 ]; then result=stop_unknown; else result=idle; fi
        return
    fi
    [ "$((stopped_count + new_count))" -le 64 ] || { result=ledger_full; return; }
    fresh_sample || { result=unknown; return; }
    regular_or_missing "$ledger" && regular_or_missing "$ledger_tmp" || { result=attention_required; return; }
    write_private "$ledger_tmp" "$updated_ledger" && /bin/mv -f "$ledger_tmp" "$ledger" || { result=attention_required; return; }
    # Flush only the owned file and its rename directory. A failed flush retains
    # the intent and never permits a stop on this pass. Perl already backs shasum.
    capture 64 8 "$flush" -MFcntl=O_RDONLY,O_NOFOLLOW -MIO::Handle -e '
use strict; use warnings;
for my $p (@ARGV) {
    sysopen(my $f, $p, O_RDONLY | O_NOFOLLOW) or exit 2;
    $f->sync or exit 3;
    close($f) or exit 4;
}
' "$ledger" "$root" || { result=attention_required; return; }
    integrity && write_status stop_requested || { result=attention_required; return; }
    fresh_sample || { result=attention_required; return; }
    capture 8192 15 "$docker" --host "$docker_host" container stop --time 10 -- "$@"
    # Exit status is not inactivity evidence. Observe the exact original IDs.
    result=confirmed_inactive
    [ "$pending" = 0 ] || result=stop_unknown
    for candidate in "$@"; do
        if ! epoch || [ "$now" -lt "$sample_start" ] || [ "$((now - sample_start))" -gt 30 ]; then result=stop_unknown; break; fi
        if ! inspect_owned "$candidate" || [ "$running" != false ]; then result=stop_unknown; break
        else case "$container_status" in exited|dead) :;; *) result=stop_unknown;; esac; fi
    done
}

observe_stops() {
    load_ledger || { result=attention_required; return; }
    [ "$stopped_count" -gt 0 ] || return
    # Absence from a filtered running list cannot prove an original ID inactive.
    # Only the immediate exact-ID observation above records that stronger fact.
    result=stop_unknown
}

[ "$#" = 1 ] && [ "$0" = "$script" ] && [ -d "$root" ] && [ ! -L "$root" ] || exit 1
printf '%s\n' "$expected_manifest" | /usr/bin/awk 'NR!=1 || length($0)!=64 || $0!~/^[a-f0-9]+$/ {bad=1} END {exit bad}' || exit 1
cd "$root" || exit 1
[ "$PWD" = "$root" ] || exit 1
capture 32 3 "$id" -u || exit 1
uid=$command_output
printf '%s\n' "$uid" | /usr/bin/awk 'NR!=1 || $0!~/^[1-9][0-9]*$/ || length($0)>10 {bad=1} END {exit bad}' || exit 1
capture 64 3 "$stat" -f '%u %Lp' "$root" || exit 1
[ "$command_output" = "$uid 700" ] || exit 1
critical_since=0
previous=0
max_gap=$((interval * 2))
max_sample_age=$max_gap
[ "$max_sample_age" -le 30 ] || max_sample_age=30

while :; do
    if ! integrity; then write_status integrity_failed; exit 1; fi
    capture 64 3 "$uname" -s -m || { write_status unknown; exit 1; }
    [ "$command_output" = 'Darwin x86_64' ] || { write_status unsupported; exit 0; }
    # hw.cputype is native host hardware, including under process translation.
    capture 32 3 "$sysctl" -n hw.cputype || { write_status unknown; exit 1; }
    case "$command_output" in
        7) :;;
        16777228) write_status unsupported; exit 0;;
        *) write_status unknown; exit 1;;
    esac
    core_known=1
    critical=0
    battery_critical=0
    result=idle
    epoch || { write_status unknown; exit 1; }
    sample_start=$now
    if capture 128 8 "$sysctl" -n machdep.xcpm.cpu_thermal_level; then
        level=$(printf '%s\n' "$command_output" | /usr/bin/awk '
            NR!=1 || $0!~/^(0|[1-9][0-9]*)$/ || length($0)>3 || $0+0>255 {bad=1}
            END {if (bad) exit 1; print $0}') || core_known=0
        if [ "$core_known" = 1 ] && [ "$level" -ge "$thermal_threshold" ]; then critical=1; fi
    else core_known=0; fi
    if capture 8192 8 "$pmset" -g therm; then
        speed=$(printf '%s\n' "$command_output" | /usr/bin/awk '
            /CPU_Speed_Limit/ {if ($0!~/^[ \t]*CPU_Speed_Limit[ \t]*=[ \t]*[0-9]+[ \t]*$/) bad=1; n++; sub(/^.*=[ \t]*/, ""); sub(/[ \t]*$/, ""); value=$0; if (value!~/^(0|[1-9][0-9]*)$/ || length(value)>3 || value+0>100) bad=1}
            END {if (bad || n!=1) exit 1; print value}') || core_known=0
        if [ -n "$speed" ] && [ "$speed" -le "$speed_threshold" ] 2>/dev/null; then critical=1; fi
    else core_known=0; fi
    if capture 8192 8 "$pmset" -g batt; then
        power=$(printf '%s\n' "$command_output" | /usr/bin/awk '
            /^Now drawing from / {sources++; if ($0=="Now drawing from '\''AC Power'\''") source="ac"; else if ($0=="Now drawing from '\''Battery Power'\''") source="battery"; else bad=1}
            /^[ \t]*-InternalBattery-/ {
                batteries++; row=$0
                sub(/^[ \t]*-InternalBattery-[0-9]+[ \t]+/, "", row)
                if (row~/^[(]id=[0-9]+[)][ \t]+/) sub(/^[(]id=[0-9]+[)][ \t]+/, "", row)
                if (row!~/^(0|[1-9][0-9]*)%;[ \t]/) bad=1
                charge=row; sub(/%;.*$/, "", charge)
                if (charge!~/^(0|[1-9][0-9]*)$/ || length(charge)>3 || charge+0>100) bad=1
                if (row!~/present: true[ \t]*$/ || gsub(/present:/, "&", row)!=1) bad=1
            }
            END {if (bad || sources!=1 || batteries>1 || (source=="battery" && batteries!=1)) exit 1; if (charge=="") charge="-"; print source " " charge}') || core_known=0
        case "$power" in
            'battery '*) charge=${power#battery }; if [ "$charge" -le "$low_battery" ] 2>/dev/null; then battery_critical=1; fi;;
        esac
    else core_known=0; fi
    if [ "$powermetrics_granted" = 1 ] && capture 8192 8 "$sudo" -n /usr/bin/powermetrics --samplers smc -n 1 -i 1000; then
        if printf '%s\n' "$command_output" | /usr/bin/awk -v threshold="$temperature_threshold" '
            /^(CPU|GPU) die temperature:/ {n++; if ($0!~/^(CPU|GPU) die temperature:[ \t]*[0-9]+([.][0-9]+)?[ \t]+C[ \t]*$/) bad=1; split($0,p,":"); sub(/^[ \t]*/, "", p[2]); split(p[2],v,/[ \t]+/); if (v[1]+0>150) bad=1; if (v[1]+0>=threshold+0) hot=1; if ($1=="CPU") cpu++; else gpu++}
            END {exit bad || cpu>1 || gpu>1 || !n || !hot}'; then critical=1; fi
    fi
    epoch || { write_status unknown; exit 1; }
    if [ "$now" -lt "$sample_start" ] || [ "$((now - sample_start))" -gt "$max_sample_age" ]; then core_known=0; battery_critical=0; fi
    if [ "$battery_critical" = 1 ]; then
        critical_since=0
        previous=0
        stop_owned
    elif [ "$core_known" = 1 ] && [ "$critical" = 1 ]; then
        if [ "$previous" = 0 ] || [ "$now" -le "$previous" ] || [ "$((now - previous))" -gt "$max_gap" ]; then critical_since=$now; fi
        previous=$now
        if [ "$((now - critical_since))" -ge "$critical_duration" ]; then stop_owned; else result=critical_wait; observe_stops; fi
    else
        critical_since=0
        previous=0
        [ "$core_known" = 1 ] || result=unknown
        observe_stops
    fi
    write_status "$result" || exit 1
    "$sample_sleep" "$interval" || exit 0
done
