diff --git a/scripts/__init__.py b/scripts/__init__.py
new file mode 100644
index 0000000000000000000000000000000000000000..1ce29343b0c86028a3aa82579992afb87ca1813f
--- /dev/null
+++ b/scripts/__init__.py
@@ -0,0 +1,9 @@
+"""Utility package for Development.i automation scripts."""
+
+# Expose the top-level modules to make ``from scripts import dev_i_pipeline`` work
+# consistently when the package is used in GitHub Actions or imported locally.
+__all__ = [
+    "dev_i_csv_last30",
+    "dev_i_pipeline",
+]
+
