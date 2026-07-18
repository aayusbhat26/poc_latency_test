import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import { Sidebar } from "@/components/layout/Sidebar";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Lakehouse UI",
  description: "Medallion Architecture Analytics Dashboard",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body className={`${inter.className} bg-background text-foreground h-screen flex overflow-hidden`}>
        <Sidebar />
        <main className="flex-1 overflow-y-auto bg-black/40">
          <div className="min-h-full w-full p-8">{children}</div>
        </main>
      </body>
    </html>
  );
}
