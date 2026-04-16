import Link from "next/link";

export function Header() {
  return (
    <header className="bg-white border-b border-gray-200 sticky top-0 z-10">
      <div className="max-w-7xl mx-auto px-6 h-14 flex items-center gap-8">
        <Link href="/" className="text-base font-bold text-gray-800 hover:text-blue-600">
          US-JP Sector
        </Link>
        <nav className="flex items-center gap-6 text-sm">
          <Link href="/" className="text-gray-600 hover:text-blue-600">
            メイン
          </Link>
          <Link href="/backtest" className="text-gray-600 hover:text-blue-600">
            バックテスト
          </Link>
        </nav>
      </div>
    </header>
  );
}
