# What the watch asks of this Plugin, and the case it answers.
PLUGIN=com.yoke.services
UNIT=services
CASE=yoke-plugin-services:the-watch.01

# std: yoke-plugin-services:the-watch.01
probe() { # <instance>
  local capability
  capability="$(sed -n 's/^ *- name: \(.*services\.list\.answer\)$/\1/p' plugin/com.yoke.services.yaml)"
  yoke plugin grant "$PLUGIN" "$capability" --instance "$1" > /dev/null || { echo "the grant was refused"; return 1; }
  yoke unit restart "$UNIT" --instance "$1" > /dev/null || { echo "the unit did not restart"; return 1; }
  sleep 3
  local said; said="$(yoke unit ask "$UNIT" services.list --instance "$1" 2>&1)" || { echo "the question was refused: $said"; return 1; }
  grep -q '"units"' <<<"$said" || { echo "the answer holds no units: $said"; return 1; }
}
