import { redirect } from "next/navigation";

export default function DatabaseSchemaPage() {
  redirect("/tools/database/tables");
}
